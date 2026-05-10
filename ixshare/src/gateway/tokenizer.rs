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
use hyper::header::HeaderValue;
use inferxlib::obj_mgr::func_mgr::ApiType;
use minijinja::{context, Environment};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::result::Result as SResult;
use std::sync::Arc;
use tokenizers::Tokenizer;

use crate::gateway::auth_layer::AccessToken;
use crate::gateway::http_gateway::FuncCall1;
use crate::gateway::http_gateway::HttpGateway;
use crate::node_config::KB_PROMPT_DIR;

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
    #[serde(default = "DefaultTemperature")]
    pub temperature: f32,
    #[serde(rename = "stream")]
    pub stream: bool,
    #[serde(default = "DefaultTopP")]
    pub topP: f32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatMessage {
    #[serde(rename = "role")]
    pub role: String,
    #[serde(rename = "content")]
    pub content: String,
}

fn DefaultTemperature() -> f32 {
    1.0
}

fn DefaultTopP() -> f32 {
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

/// Completion request that uses raw text prompt instead of tokenized IDs.
#[derive(Debug, Clone, Serialize)]
pub struct RawCompletionRequest {
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
    pub stream: bool,
    #[serde(rename = "skip_special_tokens")]
    pub skipSpecialTokens: bool,
}

// ==================================================================
// ChatTemplate helper
// ==================================================================

/// Read the chat_template, bos_token, eos_token from tokenizer_config.json.
fn LoadTokenizerConfig(modelPath: &str) -> (Option<String>, String, String) {
    let configPath = Path::new(modelPath).join("tokenizer_config.json");
    if let Ok(configStr) = std::fs::read_to_string(&configPath) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&configStr) {
            let chatTemplate = ExtractStr(&val, "chat_template");
            let bosToken = ExtractStrOrDefault(&val, "bos_token", "<|begin_of_text|>");
            let eosToken = ExtractStrOrDefault(&val, "eos_token", "<|end_of_text|>");
            return (chatTemplate, bosToken, eosToken);
        }
    }
    (
        None,
        "<|begin_of_text|>".to_string(),
        "<|end_of_text|>".to_string(),
    )
}

fn ExtractStr(val: &serde_json::Value, key: &str) -> Option<String> {
    match val.get(key).and_then(|v| v.as_str()) {
        Some(s) => Some(s.to_string()),
        None => None,
    }
}

fn ExtractStrOrDefault(val: &serde_json::Value, key: &str, default: &str) -> String {
    match val.get(key).and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => default.to_string(),
    }
}

/// Apply the Jinja chat_template from tokenizer_config.json to convert
/// `/v1/chat/completions` messages into a prompt string for `/v1/completions`.
///
/// Uses minijinja to render the template. Falls back to a built-in Llama 3
/// format when no chat_template is available.
fn ApplyChatTemplate(
    messages: &[ChatMessage],
    chatTemplate: Option<&str>,
    bosToken: &str,
) -> String {
    if let Some(templateStr) = chatTemplate {
        let mut env = Environment::new();
        env.add_template_owned("chat_template", templateStr.to_string())
            .unwrap();

        let addGenerationPrompt =
            !messages.is_empty() && messages.last().map_or(true, |m| m.role != "assistant");

        let messagesJson: Vec<serde_json::Value> = messages
            .iter()
            .map(|m| serde_json::json!({ "role": m.role, "content": m.content }))
            .collect();

        return env
            .get_template("chat_template")
            .unwrap()
            .render(context!(
                bos_token => bosToken.to_string(),
                eos_token => String::new(),
                messages => messagesJson,
                add_generation_prompt => addGenerationPrompt
            ))
            .unwrap_or_else(|_| DefaultLlama3Template(messages, bosToken));
    }

    DefaultLlama3Template(messages, bosToken)
}

/// Built-in Llama 3 chat template fallback (matches the standard
/// `<|begin_of_text|><|start_header_id|>...` format).
fn DefaultLlama3Template(messages: &[ChatMessage], bosToken: &str) -> String {
    let mut text = String::new();
    text.push_str(bosToken);

    for msg in messages {
        // Convert "bot" -> "assistant" for compatibility
        let role = if msg.role == "bot" {
            "assistant"
        } else {
            &msg.role
        };
        let eos = if role == "assistant" { "</s>" } else { "" };
        text.push_str(&format!(
            "<|start_header_id|>{role}<|end_header_id|>\n\n{content}{eos}",
            role = role,
            content = msg.content
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
    /// Jinja chatTemplate string from tokenizer_config.json
    pub chatTemplate: Option<String>,
    /// bosToken from tokenizer_config.json
    pub bosToken: String,
    /// eosToken from tokenizer_config.json
    pub eosToken: String,
}

lazy_static::lazy_static! {
    pub static ref SHARED_TOKENIZERS: DashMap<String, Arc<TokenizerState>> = DashMap::new();
}

async fn GetOrLoadTokenizer(modelPath: &str) -> SResult<Arc<TokenizerState>, StatusCode> {
    if let Some(state) = SHARED_TOKENIZERS.get(modelPath) {
        return Ok(state.value().clone());
    }

    // Load tokenizer.json (local or HF hub)
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

    // Load tokenizer_config.json for chatTemplate, bosToken, etc.
    let (chatTemplate, bosToken, eosToken): (Option<String>, String, String) =
        if Path::new(modelPath).exists() {
            LoadTokenizerConfig(modelPath)
        } else {
            // Try loading from HF hub
            let config_str = (|| -> Option<String> {
                let api = hf_hub::api::sync::Api::new().ok()?;
                let repo = api.model(modelPath.to_string());
                let file = repo.get("tokenizer_config.json").ok()?;
                std::fs::read_to_string(file).ok()
            })();
            if let Some(cs) = config_str {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&cs) {
                    let ct = ExtractStr(&val, "chat_template");
                    let bos = ExtractStrOrDefault(&val, "bos_token", "<|begin_of_text|>");
                    let eos = ExtractStrOrDefault(&val, "eos_token", "<|end_of_text|>");
                    (ct, bos, eos)
                } else {
                    (
                        None,
                        "<|begin_of_text|>".to_string(),
                        "<|end_of_text|>".to_string(),
                    )
                }
            } else {
                (
                    None,
                    "<|begin_of_text|>".to_string(),
                    "<|end_of_text|>".to_string(),
                )
            }
        };

    let state = Arc::new(TokenizerState {
        tokenizer: Arc::new(tokenizer),
        chatTemplate,
        bosToken,
        eosToken,
    });

    SHARED_TOKENIZERS.insert(modelPath.to_string(), state.clone());
    Ok(state)
}

// ==================================================================
// Shared setup
// ==================================================================

pub struct RouteContext {
    pub tenant: String,
    pub namespace: String,
    pub funcName: String,
    pub remainPath: String,
    pub bodyBytes: Vec<u8>,
    pub tokenizerState: Arc<TokenizerState>,
    pub kbPrompt: Option<String>,
}

async fn ResolveRouteContext(
    token: &Arc<AccessToken>,
    gw: &HttpGateway,
    req: axum::http::Request<axum::body::Body>,
) -> SResult<RouteContext, StatusCode> {
    let (reqParts, body) = req.into_parts();
    let pathParts: Vec<&str> = reqParts.uri.path().split('/').collect();
    if pathParts.len() < 5 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let tenant = pathParts[2].to_owned();
    let namespace = pathParts[3].to_owned();
    let funcName = pathParts[4].to_owned();

    if !token.CheckScope("inference") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    if !token.IsNamespaceInferenceUser(&tenant, &namespace) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let mut remainPath = "".to_string();
    for i in 5..pathParts.len() {
        remainPath = remainPath + "/" + pathParts[i];
    }

    let func = match gw
        .funcAgentMgr
        .objRepo
        .GetFunc(&tenant, &namespace, &funcName)
    {
        Ok(f) => f,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let model_path = match func.object.spec.ModelPath() {
        Some(p) => p,
        None => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let apiType = func.object.spec.SampleCallType();
    let kbPrompt = match apiType {
        ApiType::KnowledageBase => {
            let funcname = func.Id().replace("/", ".");
            // todo: delete this
            let parts: Vec<&str> = funcname.split('.').collect();
            let funcname = parts[..parts.len() - 1].join(".");
            let promptfilename = format!("{}/{}.data", KB_PROMPT_DIR, funcname);
            match std::fs::read_to_string(&promptfilename) {
                Ok(p) => Some(p),
                Err(e) => {
                    error!(
                        "can't open prompt file {} with error {:?}",
                        &promptfilename, e
                    );
                    None
                }
            }
        }
        _ => None,
    };

    let bodyBytes = match axum::body::to_bytes(body, FUNCCALL_MAX_BODY_BYTES).await {
        Ok(b) => b.to_vec(),
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let tokenizerState = match GetOrLoadTokenizer(&model_path).await {
        Ok(st) => st,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    Ok(RouteContext {
        tenant,
        namespace,
        funcName,
        remainPath,
        bodyBytes,
        tokenizerState,
        kbPrompt: kbPrompt,
    })
}

async fn ForwardToFuncCall(
    token: &Arc<AccessToken>,
    gw: &HttpGateway,
    ctx: &RouteContext,
    targetPath: &str,
    body: &serde_json::Value,
) -> SResult<Response, StatusCode> {
    let new_body = serde_json::to_vec(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let new_req = Request::builder()
        .method(axum::http::Method::POST)
        .uri(format!(
            "/funccall/{}/{}/{}{}",
            ctx.tenant, ctx.namespace, ctx.funcName, targetPath
        ))
        .header("content-type", "application/json")
        .body(Body::from(new_body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    FuncCall1(token, gw, new_req).await
}

#[derive(Debug, Clone, Deserialize)]
pub struct PromptReq {
    pub tenant: String,
    pub namespace: String,
    pub funcName: String,
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
                    .body(Body::from(format!(
                        "service failure: list models failed {:?}",
                        e
                    )))
                    .unwrap());
            }
        };

        let modelData: Vec<serde_json::Value> = functions
            .into_iter()
            .map(|f| {
                serde_json::json!({
                    "id": f.func.name,
                    "object": "model",
                })
            })
            .collect();

        let responseBody = serde_json::json!({
            "object": "list",
            "data": modelData
        });

        let bytes =
            serde_json::to_vec(&responseBody).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(bytes))
            .unwrap());
    } else if remainPath == "/health" && req.method() == axum::http::Method::GET {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("OK"))
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
    newReq
        .headers_mut()
        .insert("X-Inferx-Model-Call", HeaderValue::from_static("true"));

    FuncCall1(&token, &gw, newReq).await
}

pub struct ParsedRequestFields {
    pub model: String,
    pub promptText: String,
    pub maxTokens: Option<u32>,
    pub temperature: Option<f32>,
    pub topP: Option<f32>,
    pub stream: bool,
}

fn ParseRequestFields(
    bodyBytes: &[u8],
    remainPath: &str,
    chatTemplate: Option<&str>,
    bosToken: &str,
) -> SResult<ParsedRequestFields, StatusCode> {
    if remainPath.starts_with("/v1/chat/completions") {
        let chatReq: ChatRequest =
            serde_json::from_slice(bodyBytes).map_err(|_| StatusCode::BAD_REQUEST)?;
        let promptText = if chatReq.messages.is_empty() {
            "".to_string()
        } else {
            ApplyChatTemplate(&chatReq.messages, chatTemplate, bosToken)
        };
        Ok(ParsedRequestFields {
            model: chatReq.model,
            promptText,
            maxTokens: chatReq.maxTokens,
            temperature: Some(chatReq.temperature),
            topP: Some(chatReq.topP),
            stream: chatReq.stream,
        })
    } else {
        let compReq: CompletionRequest =
            serde_json::from_slice(bodyBytes).map_err(|_| StatusCode::BAD_REQUEST)?;
        Ok(ParsedRequestFields {
            model: compReq.model,
            promptText: compReq.prompt,
            maxTokens: compReq.maxTokens,
            temperature: compReq.temperature,
            topP: compReq.topP,
            stream: compReq.stream,
        })
    }
}

fn BadResp(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(body))
        .unwrap()
}

/// Forward non-POST requests directly to the function call route without transformation.
async fn ForwardNonPostToFunc(
    token: &Arc<AccessToken>,
    gw: &HttpGateway,
    req: Request<Body>,
) -> SResult<Response<Body>, StatusCode> {
    let (mut reqParts, body) = req.into_parts();
    if !token.CheckScope("inference") {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let pathParts: Vec<&str> = reqParts.uri.path().split('/').collect();
    if pathParts.len() < 5 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tenant = pathParts[2];
    let namespace = pathParts[3];
    let funcName = pathParts[4];
    if !token.IsNamespaceInferenceUser(tenant, namespace) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let _ = gw
        .funcAgentMgr
        .objRepo
        .GetFunc(tenant, namespace, funcName)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut remainPath = String::new();
    for i in 5..pathParts.len() {
        remainPath.push('/');
        remainPath.push_str(pathParts[i]);
    }

    let newUri = format!(
        "/funccall/{}/{}/{}{}",
        tenant, namespace, funcName, remainPath
    );
    reqParts.uri = Uri::try_from(newUri).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(FuncCall1(token, &gw, Request::from_parts(reqParts, body)).await?)
}

// ==================================================================
// TokenizerRoute handler
// ==================================================================

pub async fn TokenizerRoute(
    Extension(token): Extension<Arc<AccessToken>>,
    State(gw): State<HttpGateway>,
    req: axum::http::Request<axum::body::Body>,
) -> SResult<Response<Body>, StatusCode> {
    if req.method() != axum::http::Method::POST {
        return ForwardNonPostToFunc(&token, &gw, req).await;
    }

    let ctx = match ResolveRouteContext(&token, &gw, req).await {
        Ok(c) => c,
        Err(status) => {
            return Ok(Response::builder()
                .status(status)
                .body(Body::from("service failure: validation failed"))
                .unwrap());
        }
    };

    let fields = match ParseRequestFields(
        &ctx.bodyBytes,
        &ctx.remainPath,
        ctx.tokenizerState.chatTemplate.as_deref(),
        &ctx.tokenizerState.bosToken,
    ) {
        Ok(f) => f,
        Err(_) => return Ok(BadResp("service failure: invalid JSON body".into())),
    };

    let promptIds = match ctx
        .tokenizerState
        .tokenizer
        .encode(&*fields.promptText, true)
    {
        Ok(encoded) => encoded.get_ids().to_vec(),
        Err(e) => {
            return Ok(BadResp(format!("tokenization error: {}", e)));
        }
    };

    let vllmReq = VllmCompletionRequest {
        model: fields.model,
        prompt: promptIds,
        maxTokens: fields.maxTokens,
        temperature: fields.temperature,
        topP: fields.topP,
        stream: fields.stream,
        skipSpecialTokens: true,
    };

    ForwardToFuncCall(
        &token,
        &gw,
        &ctx,
        "/v1/completions",
        &serde_json::to_value(&vllmReq).unwrap(),
    )
    .await
}

// ==================================================================
// KnowledgeBaseRoute handler - converts chat to completion without tokenization
// ==================================================================

pub async fn KnowledgeBaseRoute(
    Extension(token): Extension<Arc<AccessToken>>,
    State(gw): State<HttpGateway>,
    req: axum::http::Request<axum::body::Body>,
) -> SResult<Response<Body>, StatusCode> {
    if req.method() != axum::http::Method::POST {
        return ForwardNonPostToFunc(&token, &gw, req).await;
    }

    let ctx = match ResolveRouteContext(&token, &gw, req).await {
        Ok(c) => c,
        Err(status) => {
            return Ok(Response::builder()
                .status(status)
                .body(Body::from("service failure: validation failed"))
                .unwrap());
        }
    };

    let fields = match ParseRequestFields(
        &ctx.bodyBytes,
        &ctx.remainPath,
        ctx.tokenizerState.chatTemplate.as_deref(),
        &ctx.tokenizerState.bosToken,
    ) {
        Ok(f) => f,
        Err(_) => return Ok(BadResp("service failure: invalid JSON body".into())),
    };

    let promptText = match &ctx.kbPrompt {
        None => fields.promptText.clone(),
        Some(kbPrompt) => kbPrompt.clone() + &fields.promptText,
    };

    let rawReq = RawCompletionRequest {
        model: fields.model,
        prompt: promptText,
        maxTokens: fields.maxTokens,
        temperature: fields.temperature,
        topP: fields.topP,
        stream: fields.stream,
        skipSpecialTokens: true,
    };

    ForwardToFuncCall(
        &token,
        &gw,
        &ctx,
        "/v1/completions",
        &serde_json::to_value(&rawReq).unwrap(),
    )
    .await
}
