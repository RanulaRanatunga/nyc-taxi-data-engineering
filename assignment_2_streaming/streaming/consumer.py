#!/usr/bin/env python3
import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from typing import List, Optional

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from assignment_2_streaming.streaming import metrics
from assignment_2_streaming.streaming.config import StreamingConfig, streaming_db_config
from assignment_2_streaming.streaming.events import InvalidEventError, TripEvent
from assignment_2_streaming.streaming.processor import StreamProcessor
from assignment_2_streaming.streaming.storage import StreamingStore
from assignment_2_streaming.streaming.windowing import SlidingWindowAggregator
from common.logging_utils import setup_logging

logger = logging.getLogger("streaming.consumer")

LAG_REFRESH_SEC = 5.0


class TaxiStreamConsumer:
    def __init__(self, cfg: StreamingConfig, store: StreamingStore):
        self.cfg = cfg
        self.store = store
        self.processor = StreamProcessor(store, SlidingWindowAggregator.from_config(cfg))
        self.kafka = None
        self.dlq = None
        self.max_records = cfg.poll_max_records
        self._write_latency_ewma = 0.0
        self._last_lag_refresh = 0.0
        self._stop = False

    def stop(self, *_):
        logger.info("shutdown requested")
        self._stop = True

    def _connect(self) -> None:
        from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer

        processor = self.processor

        class RebalanceListener(ConsumerRebalanceListener):
            def on_partitions_revoked(self, revoked):
                logger.info("partitions revoked", extra={"partitions": sorted(tp.partition for tp in revoked)})

            def on_partitions_assigned(self, assigned):
                logger.info("partitions assigned", extra={"partitions": sorted(tp.partition for tp in assigned)})
                processor.rehydrate()

        self.kafka = KafkaConsumer(
            bootstrap_servers=self.cfg.bootstrap_list,
            group_id=self.cfg.consumer_group,
            client_id="taxi-trip-consumer",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=self.cfg.poll_max_records,
            max_poll_interval_ms=300_000,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
            fetch_max_wait_ms=500,
        )
        self.kafka.subscribe([self.cfg.topic], listener=RebalanceListener())
        self.dlq = KafkaProducer(
            bootstrap_servers=self.cfg.bootstrap_list,
            client_id="taxi-trip-dlq-producer",
            acks="all",
            retries=5,
        )
        logger.info("subscribed", extra={"topic": self.cfg.topic, "group": self.cfg.consumer_group})

    def run(self, max_messages: Optional[int] = None) -> None:
        self.store.initialize_schema()
        self.processor.rehydrate()
        self._connect()
        consumed = 0
        try:
            while not self._stop:
                records = self.kafka.poll(timeout_ms=1000, max_records=self.max_records)
                self._refresh_lag_metrics()
                messages = [message for batch in records.values() for message in batch]
                if not messages:
                    continue

                metrics.CONSUMER_EVENTS_CONSUMED.labels(
                    topic=self.cfg.topic, consumer_group=self.cfg.consumer_group).inc(len(messages))
                metrics.CONSUMER_BATCH_SIZE.observe(len(messages))

                events = self._parse(messages)
                try:
                    result = self.processor.process(events)
                except Exception:
                    metrics.DB_WRITE_ERRORS.inc()
                    logger.exception("batch processing failed; rebuilding state and re-consuming")
                    self._recover()
                    continue

                self.dlq.flush(timeout=10)
                try:
                    self.kafka.commit()
                except Exception as exc:
                    logger.warning("offset commit failed", extra={"error": repr(exc)})

                consumed += len(messages)
                logger.debug("batch processed", extra={"messages": len(messages), **result.outcomes})
                self._apply_backpressure(result.write_seconds)
                if max_messages and consumed >= max_messages:
                    logger.info("max messages reached", extra={"consumed": consumed})
                    break
        finally:
            self._shutdown()

    def _parse(self, messages) -> List[TripEvent]:
        events = []
        for message in messages:
            try:
                event = TripEvent.from_json(message.value)
            except InvalidEventError as exc:
                metrics.EVENTS_PROCESSED.labels(status="malformed").inc()
                self._send_to_dlq(message, str(exc))
                continue
            event.kafka_partition, event.kafka_offset = message.partition, message.offset
            events.append(event)
        return events

    def _send_to_dlq(self, message, error: str) -> None:
        headers = [
            ("error", error[:500].encode("utf-8")),
            ("source_topic", message.topic.encode()),
            ("source_partition", str(message.partition).encode()),
            ("source_offset", str(message.offset).encode()),
        ]
        self.dlq.send(self.cfg.dlq_topic, key=message.key, value=message.value, headers=headers)
        metrics.DLQ_EVENTS.inc()
        logger.warning("event sent to DLQ", extra={"partition": message.partition, "offset": message.offset,
                                                   "error": error})

    def _recover(self) -> None:
        delay = 1.0
        while not self._stop:
            try:
                self.store.reconnect()
                self.processor.rehydrate()
                self._rewind_to_committed()
                return
            except Exception as exc:
                logger.error("recovery failed, retrying", extra={"retry_in_sec": delay, "error": repr(exc)})
                self._sleep(delay)
                delay = min(delay * 2, 30.0)

    def _rewind_to_committed(self) -> None:
        for tp in self.kafka.assignment():
            committed = self.kafka.committed(tp)
            offset = getattr(committed, "offset", committed)
            if offset is None:
                self.kafka.seek_to_beginning(tp)
            else:
                self.kafka.seek(tp, offset)

    def _apply_backpressure(self, write_seconds: float) -> None:
        self._write_latency_ewma = 0.3 * write_seconds + 0.7 * self._write_latency_ewma
        if self._write_latency_ewma > self.cfg.backpressure_latency_sec:
            self.max_records = max(self.cfg.poll_min_records, self.max_records // 2)
            partitions = list(self.kafka.assignment())
            logger.warning("backpressure: sink is slow, pausing fetch",
                           extra={"write_latency_ewma_sec": round(self._write_latency_ewma, 3),
                                  "max_records": self.max_records, "pause_sec": self.cfg.backpressure_pause_sec})
            metrics.BACKPRESSURE_ACTIVE.set(1)
            metrics.BACKPRESSURE_EVENTS.inc()
            self.kafka.pause(*partitions)
            self._sleep(self.cfg.backpressure_pause_sec)
            self.kafka.resume(*partitions)
            self._write_latency_ewma *= 0.5
        else:
            metrics.BACKPRESSURE_ACTIVE.set(0)
            self.max_records = min(self.cfg.poll_max_records, self.max_records * 2)
        metrics.POLL_MAX_RECORDS.set(self.max_records)

    def _refresh_lag_metrics(self) -> None:
        now = time.monotonic()
        if now - self._last_lag_refresh < LAG_REFRESH_SEC:
            return
        self._last_lag_refresh = now
        try:
            assigned = list(self.kafka.assignment())
            end_offsets = self.kafka.end_offsets(assigned) if assigned else {}
            metrics.CONSUMER_LAG_RECORDS.clear()
            for tp in assigned:
                lag = max(0, end_offsets[tp] - self.kafka.position(tp))
                metrics.CONSUMER_LAG_RECORDS.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
        except Exception as exc:
            logger.debug("lag refresh failed", extra={"error": repr(exc)})

    def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self._stop and time.monotonic() < deadline:
            time.sleep(min(0.2, deadline - time.monotonic()))

    def _shutdown(self) -> None:
        try:
            rows = self.processor.shutdown_flush()
            logger.info("open windows flushed", extra={"rows": rows})
        except Exception:
            logger.exception("final window flush failed (state will be rebuilt on restart)")
        for client in (self.dlq, self.kafka):
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
        self.store.close()
        logger.info("consumer stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC taxi Kafka stream consumer")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after N messages")
    args = parser.parse_args()

    setup_logging("stream_consumer")
    cfg = StreamingConfig()
    metrics.start_metrics_server(cfg.consumer_metrics_port)
    consumer = TaxiStreamConsumer(cfg, StreamingStore.connect(streaming_db_config()))
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
