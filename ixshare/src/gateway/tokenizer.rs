// Copyright (c) 2025 InferX Authors /
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::Extension;
use serde::{Deserialize, Serialize};
use tokenizers::Tokenizer;

use std::path::Path;
use std::result::Result as SResult;

use crate::gateway::auth_layer::AccessToken;
use crate::gateway::http_gateway::FuncCall1;
use crate::gateway::http_gateway::HttpGateway;

const FUNCCALL_MAX_BODY_BYTES: usize = 20 * 1024 * 1024;

// ==================================================================
// Types (OpenAI-compatible request shapes)
// ==================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ChatRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(rename = "messages")]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub maxTokens: Option<u32>,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(rename = "stream")]
    pub stream: bool,
    #[serde(default = "default_top_p")]
    pub topP: f32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatMessage {
    #[serde(rename = "role")]
    pub role: String,
    #[serde(rename = "content")]
    pub content: String,
}

fn default_temperature() -> f32 {
    1.0
}

fn default_top_p() -> f32 {
    1.0
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CompletionRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(rename = "prompt")]
    pub prompt: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "max_tokens"
    )]
    pub maxTokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "top_p")]
    pub topP: Option<f32>,
    #[serde(rename = "stream")]
    pub stream: bool,
}

// ==================================================================
// VllmCompletionRequest (for tokenizer output)
// ==================================================================

#[derive(Debug, Clone, Serialize)]
pub struct VllmCompletionRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(rename = "prompt")]
    pub prompt: Vec<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "max_tokens"
    )]
    pub maxTokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "top_p")]
    pub topP: Option<f32>,
    pub stream: bool,
    #[serde(rename = "skip_special_tokens")]
    pub skipSpecialTokens: bool,
}

// ==================================================================
// ChatTemplate helper
// ==================================================================

fn applyChatTemplate(messages: &Vec<ChatMessage>) -> String {
    let mut text = String::new();
    text.push_str("<|begin_of_text|>");

    for msg in messages {
        text.push_str(&format!(
            "<|start_header_id|>{}<|end_header_id|>\n\n{}</s>",
            msg.role, msg.content
        ));
    }

    text.push_str("<|start_header_id|>assistant<|end_header_id|>\n\n");
    text
}

// ==================================================================
// Shared Tokenizer State
// ==================================================================

pub struct TokenizerState {
    pub tokenizer: Arc<Tokenizer>,
}

lazy_static::lazy_static! {
    pub static ref SHARED_TOKENIZER: Option<TokenizerState> = {
        match std::env::var("TOKENIZER_PATH") {
            Ok(path) => {
                let tokenizer = if Path::new(&path).is_file() {
                    match Tokenizer::from_file(&path) {
                        Ok(t) => t,
                        Err(e) => {
                            warn!("tokenizer failed to load '{}': {}", path, e);
                            return None;
                        }
                    }
                } else if Path::new(&path).join("tokenizer.json").is_file() {
                    match Tokenizer::from_file(&format!("{}/tokenizer.json", path)) {
                        Ok(t) => t,
                        Err(e) => {
                            warn!("tokenizer failed to load dir '{}': {}", path, e);
                            return None;
                        }
                    }
                } else {
                    warn!("tokenizer path '{}' not found", path);
                    return None;
                };
                let vocab_size = tokenizer.get_vocab_size(false);
                info!("tokenizer loaded: {} (vocab_size={})", path, vocab_size);
                Some(TokenizerState {
                    tokenizer: Arc::new(tokenizer),
                })
            }
            Err(_) => None,
        }
    };
}

// ==================================================================
// TokenizerRoute handler
// ==================================================================

pub async fn TokenizerRoute(
    Extension(token): Extension<Arc<AccessToken>>,
    State(gw): State<HttpGateway>,
    req: axum::http::Request<axum::body::Body>,
) -> SResult<Response<Body>, StatusCode> {
    let path = req.uri().path();
    let parts: Vec<&str> = path.split('/').collect();
    let partsCount = parts.len();

    if partsCount < 5 {
        let body = Body::from("service failure: Invalid input");
        let resp = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(body)
            .unwrap();
        return Ok(resp);
    }

    // Path layout: /tokenizer/:tenant/:namespace/:funcname/*rest
    let tenant = parts[2].to_owned();
    let namespace = parts[3].to_owned();
    let funcname = parts[4].to_owned();

    if !token.CheckScope("inference") {
        let body = Body::from("service failure: insufficient scope");
        let resp = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(body)
            .unwrap();
        return Ok(resp);
    }

    if !token.IsNamespaceInferenceUser(&tenant, &namespace) {
        let body = Body::from("service failure: No permission");
        let resp = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(body)
            .unwrap();
        return Ok(resp);
    }

    // Extract remainPath from path[5:]
    let mut remainPath = "".to_string();
    for i in 5..partsCount {
        remainPath = remainPath + "/" + parts[i];
    }

    // Get the body bytes
    let bytes = match axum::body::to_bytes(req.into_parts().1, FUNCCALL_MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            let body = Body::from("service failure: failed to read body");
            let resp = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(body)
                .unwrap();
            return Ok(resp);
        }
    };

    let tokenizerState = match SHARED_TOKENIZER.as_ref() {
        Some(st) => st,
        None => {
            error!("Tokenizer not configured");
            let body = Body::from("service failure: tokenizer not configured");
            let resp = Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(body)
                .unwrap();
            return Ok(resp);
        }
    };

    // Determine endpoint type based on path and build VllmCompletionRequest
    let vllmReq: VllmCompletionRequest;
    let targetPath: String;

    if remainPath.starts_with("/v1/chat/completions") {
        // Chat endpoint: /v1/chat/completions
        let chatReq: ChatRequest = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(_) => {
                let body = Body::from("service failure: invalid JSON body");
                let resp = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap();
                return Ok(resp);
            }
        };

        // Apply chat template
        let formattedText = if chatReq.messages.is_empty() {
            "".to_string()
        } else {
            applyChatTemplate(&chatReq.messages)
        };

        // Encode via tokenizer
        let promptIds = match tokenizerState.tokenizer.encode(&*formattedText, true) {
            Ok(encoded) => encoded.get_ids().to_vec(),
            Err(e) => {
                let body = Body::from(format!("tokenization error: {}", e));
                let resp = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap();
                return Ok(resp);
            }
        };

        // info!(
        //     "tokenized {} messages -> {} tokens (chat endpoint)",
        //     chatReq.messages.len(),
        //     promptIds.len()
        // );

        vllmReq = VllmCompletionRequest {
            model: chatReq.model.clone(),
            prompt: promptIds,
            maxTokens: chatReq.maxTokens,
            temperature: if (chatReq.temperature - 1.0).abs() > f32::EPSILON {
                Some(chatReq.temperature)
            } else {
                None
            },
            topP: if (chatReq.topP - 1.0).abs() > f32::EPSILON {
                Some(chatReq.topP)
            } else {
                None
            },
            stream: chatReq.stream,
            skipSpecialTokens: true,
        };
    } else {
        // Completion endpoint: /v1/completions
        let compReq: CompletionRequest = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(_) => {
                let body = Body::from("service failure: invalid JSON body");
                let resp = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap();
                return Ok(resp);
            }
        };
        // Encode prompt directly (no chat template)
        let promptIds = match tokenizerState.tokenizer.encode(&*compReq.prompt, true) {
            Ok(encoded) => encoded.get_ids().to_vec(),
            Err(e) => {
                let body = Body::from(format!("tokenization error: {}", e));
                let resp = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap();
                return Ok(resp);
            }
        };

        info!(
            "tokenized prompt length {} -> {} tokens (completion endpoint)",
            compReq.prompt.len(),
            promptIds.len()
        );

        vllmReq = VllmCompletionRequest {
            model: compReq.model.clone(),
            prompt: promptIds,
            maxTokens: compReq.maxTokens,
            temperature: compReq.temperature,
            topP: compReq.topP,
            stream: compReq.stream,
            skipSpecialTokens: true,
        };
    }
    targetPath = "/v1/completions".to_string();

    // Create new request and call FuncCall1 directly (no HTTP forward)
    let new_body = serde_json::to_vec(&vllmReq).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let new_req = axum::http::Request::builder()
        .method(axum::http::Method::POST)
        .uri(format!(
            "/funccall/{}/{}/{}{}",
            tenant, namespace, funcname, targetPath
        ))
        .header("content-type", "application/json")
        .body(Body::from(new_body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    FuncCall1(&token, &gw, new_req).await
}
