use std::{env, error::Error};

use aws_config::load_from_env;
use aws_sdk_s3::Client;
use dotenvy::dotenv;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use crate::aggregator::{db_utils::aggregate_job_enqueue_fail, value::{AggregateServiceError, Task}};



pub async fn run() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("Failed to connect to DB");

    let config = load_from_env().await;
    let client = Client::new(&config);

    loop {
        match get_aggregate_task().await {
            Ok(Some(val)) => {
                dbg!(&val);
                if let Err(e) = process(val.clone(), &db.clone()).await {
                    eprintln!("Error while splitting {}", e);
                    fail_job(&db.clone(), val).await;
                }
                continue;
            },
            Ok(None) => {
                continue;
            },
            Err(_) => {
                eprintln!("error");
                continue;
            }
        }
    }
}


async fn process(task: Task, db: &Pool<Postgres> ) -> Result<(), AggregateServiceError> {
    //TODO: Fetch all results from results table in db using root_job_id as job_id. combine all the
    //results and create a .txt file? and store that file in s3. Store the file key in job table in
    //result_url column and update status to completed, When user tries to fetch the result, if
    //status completed download file from s3 using aws creds. and send back to user
    Ok(())
}

async fn get_aggregate_task() -> Result<Option<Task>, AggregateServiceError> {
    let client  = reqwest::Client::new();
    let url = "http://127.0.0.1:8080/task?task_type=ocr&timeout=10";
    let res = client.get(url)
        .send()
        .await
        .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
    if res.status().is_success() {
        let task: Option<Task> = res
            .json()
            .await
            .map_err(|e| AggregateServiceError::Failed(e.to_string()))?;
        Ok(task)
    } else {
        Err(AggregateServiceError::Failed("INTERNAL SERVICE ERROR".to_string()))
    }
}

async fn fail_job(db: &Pool<Postgres>, task: Task) {
    let task = Task {
        job_id: task.job_id.clone(),
        task_type: "aggregate".to_string(),
        retry_left: task.retry_left - 1,
        root_job_id: task.root_job_id
    };
    let client  = reqwest::Client::new();
    let url = "http://127.0.0.1:8080/push";
    match client.post(url).json(&task).send().await {
        Ok(_) => (),
        Err(_) => {
            let _ = aggregate_job_enqueue_fail(db, &task.job_id).await.map_err(|e| AggregateServiceError::Failed(e.to_string()));
        }
    }
}
