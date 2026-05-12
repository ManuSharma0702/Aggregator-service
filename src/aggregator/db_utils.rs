use std::str::FromStr;

use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::aggregator::value::AggregateServiceError;

pub async fn aggregate_job_enqueue_fail(db_conn: &Pool<Postgres>, job_id: &str) -> Result<(), AggregateServiceError> {
    let uuid = Uuid::from_str(job_id)
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    
    sqlx::query(
        r#"
        UPDATE ocr_jobs
        SET
            enqueue_left = GREATEST(enqueue_left - 1, 0),
            status = CASE
                WHEN enqueue_left - 1 <= 0 THEN 'dead'
                ELSE 'ocr_enqueue_failed'
            END
        WHERE id = $1
        "#
    )
    .bind(uuid)
    .execute(db_conn)
    .await
    .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;

    Ok(())
}

