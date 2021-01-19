SELECT
  @submission_date AS base_date,
  *,
FROM
  ML.FORECAST(
    MODEL ml_models.telemetry_error_counts_v1,
    STRUCT<horizon INT64, confidence_level FLOAT64>(336, 0.99)
  )
