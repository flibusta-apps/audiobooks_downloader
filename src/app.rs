use axum::{body::Body, routing::RouterIntoService};
use tower::Layer;
use tower_http::normalize_path::{NormalizePath, NormalizePathLayer};

use crate::{api, state::AppState};

pub fn build_app(state: AppState) -> NormalizePath<RouterIntoService<Body>> {
    NormalizePathLayer::trim_trailing_slash().layer(api::router(state).into_service::<Body>())
}
