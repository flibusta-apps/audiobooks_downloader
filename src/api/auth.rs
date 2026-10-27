use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

use subtle::ConstantTimeEq;

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

    let valid = api_key
        .map(|provided| {
            provided
                .as_bytes()
                .ct_eq(state.config.api_key.as_bytes())
                .into()
        })
        .unwrap_or(false);

    if !valid {
        return Err(AppError::Unauthorized);
    }

    Ok(next.run(request).await)
}
