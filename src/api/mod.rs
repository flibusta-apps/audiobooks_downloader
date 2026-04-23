pub mod auth;
pub mod handlers;
pub mod models;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};

use crate::state::AppState;

use self::{
    auth::require_api_key,
    handlers::{
        create_download, delete_download, download_file, get_download, healthz, list_files,
        stream_file,
    },
};

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/downloads", post(create_download))
        .route(
            "/downloads/{id}",
            get(get_download).delete(delete_download),
        )
        .route("/downloads/{id}/files", get(list_files))
        .route("/downloads/{id}/files/{file_index}", get(download_file))
        .route(
            "/downloads/{id}/files/{file_index}/stream",
            get(stream_file),
        );

    let protected_api =
        Router::new()
            .nest("/api/v1", protected)
            .layer(middleware::from_fn_with_state(
                state.clone(),
                require_api_key,
            ));

    Router::new()
        .route("/healthz", get(healthz))
        .merge(protected_api)
        .with_state(state)
}
