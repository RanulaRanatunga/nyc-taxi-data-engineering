SELECT
    batch_id,
    source_file,
    status,
    stage,
    rows_extracted,
    rows_cleaned,
    rows_rejected,
    rows_loaded,
    started_at,
    finished_at,
    execution_duration_sec,
    error_message
FROM etl_audit_log
ORDER BY started_at DESC
LIMIT 20;
