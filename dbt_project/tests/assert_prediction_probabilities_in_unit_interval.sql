-- P(delay) must lie in [0, 1].
SELECT *
FROM {{ source('ml_pipeline', 'ml_predictions') }}
WHERE predicted_probability < 0
   OR predicted_probability > 1
   OR predicted_probability IS NULL
