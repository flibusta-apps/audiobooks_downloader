use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

use crate::{error::AppError, state::AppState};

pub async fn require_api_key(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    let api_key = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());

    if api_key != Some(state.config.api_key.as_str()) {
        return Err(AppError::Unauthorized);
    }

    Ok(next.run(request).await)
}
