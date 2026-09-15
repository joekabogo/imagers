use aws_config::SdkConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::presigning::PresigningConfig;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use lapin::options::BasicPublishOptions;
use lapin::types::ShortString;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use std::{env, process};
use tokio::net::TcpListener;
use uuid::Uuid;
#[derive(Clone, Serialize)]
enum UploadStatus {
    Success,
    Failed,
}
#[derive(Clone)]
struct AppState {
    connection: Arc<Connection>,
    config: Config,
    client: Arc<Client>,
}
#[derive(Serialize, Deserialize)]
struct Message {
    content_type: String,
    image_key: String,
}
#[derive(Serialize, Deserialize)]
struct Image {
    original_url: String,
    thumbnail: String,
}
#[derive(Serialize, Clone)]
struct UploadResponse {
    status: UploadStatus,
    message: String,
}
impl IntoResponse for UploadResponse {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}
#[derive(Clone)]
struct Config {
    rabbitmq_host: String,
    upload_bucket: String,
    bucket: String,
    processing_bucket: String,
    aws_config: SdkConfig,
}
#[tokio::main]
async fn main() {
    let config = configure().await;
    let connection = Connection::connect(&config.rabbitmq_host, ConnectionProperties::default())
        .await
        .unwrap();
    let client = Client::new(&config.aws_config);
    let app_state = AppState {
        config,
        connection: Arc::new(connection),
        client: Arc::new(client),
    };
    let router = Router::new()
        .route("/upload", post(upload_handler))
        .route("/images", get(images_handler))
        .with_state(app_state);
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, router).await.unwrap()
}

async fn upload_handler(
    State(app_state): State<AppState>,
    mut multipart: Multipart,
) -> Result<UploadResponse, StatusCode> {
    while let Ok(Some(field)) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)
    {
        let content_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_string();
        let extension =
            extension_from_content_type(&content_type).ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?;
        let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
        let filename = format!(
            "{}_{}.{}",
            Utc::now().format("%Y%m%d_"),
            Uuid::new_v4(),
            extension
        );
        let key = format!("{}/{}", app_state.config.upload_bucket, filename);
        //upload image to s3 then  add message to rabbitmq queue
        app_state
            .client
            .put_object()
            .bucket(app_state.config.bucket)
            .key(&key)
            .content_type(&content_type)
            .body(data.into())
            .send()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let message = Message {
            content_type,
            image_key: key,
        };
        send_message(app_state.connection.clone(), message)
            .await
            .unwrap();
        return Ok(UploadResponse {
            status: UploadStatus::Success,
            message: String::from("image uploaded"),
        });
    }
    Err(StatusCode::BAD_REQUEST)
}
async fn images_handler(State(app_state): State<AppState>) -> Json<Vec<Image>> {
    let processing = app_state
        .client
        .list_objects_v2()
        .bucket(&app_state.config.bucket)
        .prefix(&app_state.config.processing_bucket)
        .send()
        .await
        .unwrap();
    let presigned_config = PresigningConfig::expires_in(Duration::from_mins(5)).unwrap();
    let mut i: Vec<Image> = vec![];
    for process in processing.contents() {
        if let Some(key) = process.key() {
            let original_prefix = format!("{}/original", &app_state.config.processing_bucket);
            let thumbnail_prefix = format!("{}/thumbnail", &app_state.config.processing_bucket);
            let thumbnail_key = key.replacen(&original_prefix, &thumbnail_prefix, 1);
            let original_request = app_state
                .client
                .get_object()
                .bucket(&app_state.config.bucket)
                .key(key)
                .presigned(presigned_config.clone())
                .await
                .unwrap();
            let thumbnail_request = app_state
                .client
                .get_object()
                .bucket(&app_state.config.bucket)
                .key(thumbnail_key)
                .presigned(presigned_config.clone())
                .await
                .unwrap();
            i.push(Image {
                original_url: original_request.uri().to_string(),
                thumbnail: thumbnail_request.uri().to_string(),
            })
        }
    }
    Json(i)
}
async fn configure() -> Config {
    dotenvy::dotenv().ok();
    let config = aws_config::load_from_env().await;
    Config {
        processing_bucket: get_env_or_fail("PROCESSING_PREFIX"),
        rabbitmq_host: get_env_or_fail("RABBITMQ_HOST"),
        upload_bucket: get_env_or_fail("UPLOAD_PREFIX"),
        bucket: get_env_or_fail("BUCKET"),
        aws_config: config,
    }
}

fn get_env_or_fail(key: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("{key} environment variable is not set");
            process::exit(1);
        }
    }
}
fn extension_from_content_type(content_type: &str) -> Option<&'static str> {
    match content_type {
        "image/jpeg" => Some("jpg"),
        "image/png" => Some("png"),
        _ => None,
    }
}
async fn send_message(connection: Arc<Connection>, message: Message) -> Result<String, bool> {
    let payload = serde_json::to_vec(&message).unwrap();
    let channel = connection.create_channel().await.unwrap();
    channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from("image_queue"),
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await
        .unwrap();
    Ok("message send".to_string())
}
