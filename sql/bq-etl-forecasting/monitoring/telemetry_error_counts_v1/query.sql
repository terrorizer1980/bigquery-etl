SELECT
  TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
  document_type,
  document_version,
  error_type,
  exception_class,
  COUNT(*) AS error_count,
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.telemetry`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_hour,
  document_type,
  document_version,
  error_type,
  exception_class
ORDER BY
  submission_hour
