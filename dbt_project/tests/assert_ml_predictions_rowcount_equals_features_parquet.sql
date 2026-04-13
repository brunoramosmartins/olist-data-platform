-- Row count in ml_predictions must match features export (same scoring universe).
WITH feat AS (
    SELECT COUNT(*)::BIGINT AS n
    FROM read_parquet('../data/ml/features.parquet')
),
pred AS (
    SELECT COUNT(*)::BIGINT AS n
    FROM {{ source('ml_pipeline', 'ml_predictions') }}
)
SELECT feat.n AS features_rows, pred.n AS predictions_rows
FROM feat
CROSS JOIN pred
WHERE feat.n != pred.n
