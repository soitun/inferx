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

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{StatusCode, Uri};
use axum::response::Response;
use axum::Extension;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::result::Result as SResult;
use std::sync::Arc;
use tokenizers::Tokenizer;
use hyper::header::HeaderValue;

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
    pub static ref SHARED_TOKENIZERS: DashMap<String, Arc<TokenizerState>> = DashMap::new();
}

async fn GetOrLoadTokenizer(modelPath: &str) -> SResult<Arc<TokenizerState>, StatusCode> {
    if let Some(state) = SHARED_TOKENIZERS.get(modelPath) {
        return Ok(state.value().clone());
    }

    let tokenizer = if Path::new(modelPath).exists() {
        let tokenizerJson = Path::new(modelPath).join("tokenizer.json");
        Tokenizer::from_file(tokenizerJson).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        let api = hf_hub::api::sync::Api::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let repo = api.model(modelPath.to_string());
        let file = repo
            .get("tokenizer.json")
            .map_err(|_| StatusCode::NOT_FOUND)?;
        Tokenizer::from_file(file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };

    let state = Arc::new(TokenizerState {
        tokenizer: Arc::new(tokenizer),
    });

    SHARED_TOKENIZERS.insert(modelPath.to_string(), state.clone());
    Ok(state)
}


// ==================================================================
// TokenizerRoute handler
// ==================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct PromptReq {
    pub tenant: String,
    pub namespace: String,
    pub funcname: String,
    pub prompt: String,
    pub image: Option<String>,
}

pub async fn ModelsFuncCall(
    Extension(token): Extension<Arc<AccessToken>>,
    State(gw): State<HttpGateway>,
    req: Request,
) -> SResult<Response, StatusCode> {
    let path = req.uri().path().to_string();
    let parts = path.split('/').collect::<Vec<&str>>();
    if parts.len() < 4 {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("Invalid path"))
            .unwrap());
    }

    let tenant = parts[2].to_owned();
    let namespace = parts[3].to_owned();

    let mut remainPath = "".to_string();
    if parts.len() > 4 {
        for i in 4..parts.len() {
            remainPath = remainPath + "/" + parts[i];
        }
    }

    if remainPath == "/v1/models" && req.method() == axum::http::Method::GET {
        let functions = match gw.objRepo.ListFunc(&tenant, &namespace) {
            Ok(funcs) => funcs,
            Err(e) => {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(format!("service failure: list models failed {:?}", e)))
                    .unwrap());
            }
        };

        let model_data: Vec<serde_json::Value> = functions
            .into_iter()
            .map(|f| {
                serde_json::json!({
                    "id": f.func.name,
                    "object": "model",
                })
            })
            .collect();

        let response_body = serde_json::json!({
            "object": "list",
            "data": model_data
        });

        let bytes = serde_json::to_vec(&response_body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(bytes))
            .unwrap());
    }

    let (mut reqParts, body) = req.into_parts();
    reqParts.headers.remove(hyper::header::CONTENT_LENGTH);
    let bytes = match axum::body::to_bytes(body, FUNCCALL_MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let mut jsonReq: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    let modelName = jsonReq
        .as_object_mut()
        .and_then(|obj| obj.remove("model"))
        .and_then(|val| val.as_str().map(|s| s.to_string()))
        .ok_or(StatusCode::BAD_REQUEST)?;

    let bytes = serde_json::to_vec(&jsonReq).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let finalPath = format!(
        "/funccall/{}/{}/{}{}",
        tenant, namespace, modelName, remainPath
    );

    let mut newReq = Request::from_parts(reqParts, Body::from(bytes));
    *newReq.uri_mut() = Uri::try_from(finalPath).unwrap();
    newReq
        .headers_mut()
        .insert("X-Inferx-Model", HeaderValue::from_str(&modelName).unwrap());
    newReq.headers_mut().insert(
        "X-Inferx-Model-Call", 
        HeaderValue::from_static("true")
    );

    FuncCall1(&token, &gw, newReq).await
}

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

    let mut remainPath = "".to_string();
    for i in 5..partsCount {
        remainPath = remainPath + "/" + parts[i];
    }

    let func = match gw
        .funcAgentMgr
        .objRepo
        .GetFunc(&tenant, &namespace, &funcname)
    {
        Ok(f) => f,
        Err(e) => {
            let errcode = StatusCode::INTERNAL_SERVER_ERROR;
            let body = Body::from(format!("service failure {:?}", &e));
            let resp = Response::builder().status(errcode).body(body).unwrap();
            return Ok(resp);
        }
    };

    let model_path = match func.object.spec.ModelPath() {
        Some(path) => path,
        None => {
            let body = Body::from("service failure: --model argument is required");
            let resp = Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .unwrap();
            return Ok(resp);
        }
    };

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

    let tokenizerState = match GetOrLoadTokenizer(&model_path).await {
        Ok(st) => st,
        Err(status) => {
            let body = Body::from(format!(
                "service failure: failed to load tokenizer for {}",
                model_path
            ));
            let resp = Response::builder().status(status).body(body).unwrap();
            return Ok(resp);
        }
    };

    let vllmReq: VllmCompletionRequest;
    let targetPath: String;

    if remainPath.starts_with("/v1/chat/completions") {
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

        let formattedText = if chatReq.messages.is_empty() {
            "".to_string()
        } else {
            applyChatTemplate(&chatReq.messages)
        };

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

