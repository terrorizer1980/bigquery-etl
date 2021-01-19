CREATE OR REPLACE MODEL ml_models.telemetry_error_counts_v1
OPTIONS
  (
    MODEL_TYPE = 'ARIMA',
    TIME_SERIES_TIMESTAMP_COL = 'submission_hour',
    TIME_SERIES_DATA_COL = 'error_count',
    TIME_SERIES_ID_COL = 'exception_class',
    HORIZON = 168
  )
AS
SELECT
  submission_hour,
  exception_class,
  AVG(error_count) OVER (
    PARTITION BY
      exception_class
    ORDER BY
      UNIX_SECONDS(submission_hour) / 3600 ASC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
  ) error_count
FROM
  (
    SELECT
      submission_hour,
      REGEXP_REPLACE(exception_class, r'.*[.$](.*)', r'\1') AS exception_class,
      SUM(error_count) AS error_count,
    FROM
      monitoring.telemetry_error_counts_v1
    WHERE
      DATE(submission_hour) BETWEEN DATE_SUB(@submission_date, INTERVAL 1 MONTH) AND @submission_date
    GROUP BY
      submission_hour,
      exception_class
  )
ORDER BY
  submission_hour