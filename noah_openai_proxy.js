/**
 * Noah AI → OpenAI 兼容格式适配器
 *
 * 将 Noah AI (ai.noahgroup.com) 的接口封装为 OpenAI /v1/chat/completions 格式
 * 支持流式 (stream: true) 和非流式响应
 *
 * 启动方式: node noah_openai_proxy.js
 * 默认监听端口: 9876 (可通过环境变量 PORT 修改)
 *
 * 使用方式 (与 OpenAI SDK 兼容):
 *   baseURL: "http://localhost:9876/v1"
 *   apiKey: 你的 NOAH_AI_AUTH_TOKEN
 *   model: "gpt-5.5-thinking" (或其他 Noah AI 支持的模型)
 */

const http = require("http");
const https = require("https");
const { URL } = require("url");
const crypto = require("crypto");

const PORT = process.env.PORT || 9876;
const NOAH_BASE = "https://ai.noahgroup.com";
const DEFAULT_GPTS_ID = 76;

// ============ 工具函数 ============

function generateRequestId() {
  return crypto.randomUUID();
}

function generateChatRequestId(workNo) {
  return `${workNo}${Date.now()}${Math.floor(Math.random() * 1000)}`;
}

function parseAuthToken(token) {
  try {
    const payload = Buffer.from(token.split(".")[1], "base64").toString("utf-8");
    return JSON.parse(payload);
  } catch {
    return {};
  }
}

function buildCookieHeader(authToken, extraCookies) {
  let cookie = `NOAH_AI_AUTH_TOKEN=${authToken}`;
  if (extraCookies) {
    cookie += `; ${extraCookies}`;
  }
  return cookie;
}

function httpsRequest(url, options, body) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const reqOptions = {
      hostname: parsedUrl.hostname,
      port: 443,
      path: parsedUrl.pathname + parsedUrl.search,
      method: options.method || "POST",
      headers: options.headers || {},
    };

    const req = https.request(reqOptions, (res) => {
      if (options.stream) {
        resolve(res);
        return;
      }
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, data: data });
        }
      });
    });

    req.on("error", reject);
    if (body) req.write(typeof body === "string" ? body : JSON.stringify(body));
    req.end();
  });
}

// ============ Noah AI 接口封装 ============

async function createSession(authToken, gptsId = DEFAULT_GPTS_ID, extraCookies = "") {
  const url = `${NOAH_BASE}/api/noah-chat-svc/session/createSession`;
  const headers = {
    "Content-Type": "application/json",
    Cookie: buildCookieHeader(authToken, extraCookies),
    Origin: NOAH_BASE,
    Referer: `${NOAH_BASE}/home`,
    "User-Agent": "Mozilla/5.0 (compatible; NoahAI-Proxy/1.0)",
  };

  const body = { gptsId, sessionName: "api-session" };
  const res = await httpsRequest(url, { method: "POST", headers }, body);

  if (res.status === 200 && res.data && res.data.code === 200 && res.data.data) {
    return res.data.data.sessionId || res.data.data.id || res.data.data;
  }
  throw new Error(`createSession failed: ${JSON.stringify(res.data)}`);
}

async function streamChat(authToken, sessionId, content, model, res, extraCookies = "") {
  const tokenInfo = parseAuthToken(authToken);
  const workNo = tokenInfo.workNo || "unknown";

  const url = `${NOAH_BASE}/api/noah-chat-svc/chat/streamChat`;
  const headers = {
    "Content-Type": "application/json",
    Cookie: buildCookieHeader(authToken, extraCookies),
    Origin: NOAH_BASE,
    Referer: `${NOAH_BASE}/home`,
    "User-Agent": "Mozilla/5.0 (compatible; NoahAI-Proxy/1.0)",
    "request-id": generateRequestId(),
  };

  const body = {
    sessionId: sessionId,
    chatType: 0,
    content: content,
    currentGptsId: "",
    assistantType: 0,
    model: model || "gpt-5.5-thinking",
    pluginIds: [2, 3],
    fileIds: [],
    chatRequestId: generateChatRequestId(workNo),
  };

  const upstream = await httpsRequest(url, { method: "POST", headers, stream: true }, body);
  return upstream;
}

// ============ OpenAI 格式转换 ============

function buildOpenAIChunk(id, model, content, finishReason = null) {
  const chunk = {
    id: `chatcmpl-${id}`,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model: model,
    choices: [
      {
        index: 0,
        delta: content ? { content } : {},
        finish_reason: finishReason,
      },
    ],
  };
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

function buildOpenAIResponse(id, model, content) {
  return {
    id: `chatcmpl-${id}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: model,
    choices: [
      {
        index: 0,
        message: { role: "assistant", content },
        finish_reason: "stop",
      },
    ],
    usage: {
      prompt_tokens: 0,
      completion_tokens: 0,
      total_tokens: 0,
    },
  };
}

// ============ SSE 解析 (Noah → OpenAI) ============

class NoahSSEParser {
  constructor(onChunk, onDone, onError) {
    this.buffer = "";
    this.onChunk = onChunk;
    this.onDone = onDone;
    this.onError = onError;
  }

  feed(rawData) {
    this.buffer += rawData;
    // SSE 事件以 \n\n 分隔
    const events = this.buffer.split("\n\n");
    // 最后一段可能不完整，保留在 buffer
    this.buffer = events.pop() || "";

    for (const event of events) {
      this._processEvent(event.trim());
    }
  }

  flush() {
    if (this.buffer.trim()) {
      this._processEvent(this.buffer.trim());
      this.buffer = "";
    }
  }

  _processEvent(eventBlock) {
    if (!eventBlock) return;
    const lines = eventBlock.split("\n");
    let eventType = "";
    let dataLine = "";

    for (const line of lines) {
      if (line.startsWith("event:")) {
        eventType = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        dataLine = line.slice(5).trim();
      } else if (line.startsWith("id:")) {
        // ignore id field
      }
    }

    if (eventType === "end" || eventType === "stop" || eventType === "done") {
      this.onDone();
      return;
    }

    if (eventType === "error") {
      this.onError(new Error(dataLine || "upstream error"));
      return;
    }

    if (dataLine === "[DONE]") {
      this.onDone();
      return;
    }

    if (!dataLine) return;

    try {
      const parsed = JSON.parse(dataLine);
      let content = "";
      if (parsed.chunk) content = parsed.chunk;
      else if (parsed.content) content = parsed.content;
      else if (parsed.data && parsed.data.content) content = parsed.data.content;
      else if (parsed.choices && parsed.choices[0]) {
        const delta = parsed.choices[0].delta || parsed.choices[0].message;
        if (delta && delta.content) content = delta.content;
      }
      if (content) this.onChunk(content);
    } catch {
      // 非JSON data行，忽略
    }
  }
}

// ============ HTTP Server ============

async function handleChatCompletions(req, res) {
  let body = "";
  await new Promise((resolve) => {
    req.on("data", (chunk) => (body += chunk));
    req.on("end", resolve);
  });

  let params;
  try {
    params = JSON.parse(body);
  } catch {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: { message: "Invalid JSON body" } }));
    return;
  }

  // 提取 auth token (从 Authorization header 或 x-auth-token)
  const authHeader = req.headers["authorization"] || "";
  const authToken =
    req.headers["x-auth-token"] ||
    (authHeader.startsWith("Bearer ") ? authHeader.slice(7) : authHeader);

  if (!authToken) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: { message: "Missing auth token. Use Authorization: Bearer <NOAH_AI_AUTH_TOKEN>" } }));
    return;
  }

  // 额外 cookie：可通过 x-extra-cookies header 或 body 中的 extra_cookies 传入
  const extraCookies = req.headers["x-extra-cookies"] || params.extra_cookies || "";

  const model = params.model || "gpt-5.5-thinking";
  const stream = params.stream || false;
  const messages = params.messages || [];

  // 取最后一条 user 消息作为发送内容；如果有多轮则拼接
  let content = "";
  if (messages.length === 1) {
    content = messages[0].content;
  } else {
    // 多轮对话拼接
    content = messages
      .filter((m) => m.role === "user" || m.role === "system")
      .map((m) => (m.role === "system" ? `[系统提示] ${m.content}` : m.content))
      .join("\n\n");
  }

  if (!content) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: { message: "No message content found" } }));
    return;
  }

  const completionId = crypto.randomUUID().replace(/-/g, "").slice(0, 24);

  try {
    // Step 1: 创建会话
    const gptsId = params.gpts_id || DEFAULT_GPTS_ID;
    const sessionId = await createSession(authToken, gptsId, extraCookies);

    // Step 2: 发送消息并获取流式响应
    const upstream = await streamChat(authToken, sessionId, content, model, res, extraCookies);

    if (stream) {
      // 流式模式 → 转为 OpenAI SSE 格式
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "Access-Control-Allow-Origin": "*",
      });

      // 发送 role chunk
      res.write(
        `data: ${JSON.stringify({
          id: `chatcmpl-${completionId}`,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model,
          choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
        })}\n\n`
      );

      const parser = new NoahSSEParser(
        (content) => {
          if (!res.writableEnded) {
            res.write(buildOpenAIChunk(completionId, model, content));
          }
        },
        () => {
          if (!res.writableEnded) {
            res.write(buildOpenAIChunk(completionId, model, null, "stop"));
            res.write("data: [DONE]\n\n");
            res.end();
          }
        },
        (err) => {
          if (!res.writableEnded) {
            res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`);
            res.write("data: [DONE]\n\n");
            res.end();
          }
        }
      );

      upstream.on("data", (chunk) => {
        parser.feed(chunk.toString("utf-8"));
      });

      upstream.on("end", () => {
        parser.flush();
        if (!res.writableEnded) {
          res.write(buildOpenAIChunk(completionId, model, null, "stop"));
          res.write("data: [DONE]\n\n");
          res.end();
        }
      });

      upstream.on("error", (err) => {
        if (!res.writableEnded) {
          res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`);
          res.end();
        }
      });
    } else {
      // 非流式模式 → 收集完整响应后返回
      let fullContent = "";

      await new Promise((resolve, reject) => {
        const parser = new NoahSSEParser(
          (content) => { fullContent += content; },
          () => { resolve(); },
          (err) => { reject(err); }
        );

        upstream.on("data", (chunk) => {
          parser.feed(chunk.toString("utf-8"));
        });
        upstream.on("end", () => {
          parser.flush();
          resolve();
        });
        upstream.on("error", reject);
      });

      res.writeHead(200, {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      });
      res.end(JSON.stringify(buildOpenAIResponse(completionId, model, fullContent)));
    }
  } catch (err) {
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: { message: err.message, type: "server_error" } }));
  }
}

function handleModels(req, res) {
  const models = {
    object: "list",
    data: [
      { id: "gpt-5.5-thinking", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "gpt-4o", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "gpt-4o-mini", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "deepseek-r1", object: "model", created: 1700000000, owned_by: "noah-ai" },
    ],
  };
  res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify(models));
}

const server = http.createServer(async (req, res) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-auth-token",
    });
    res.end();
    return;
  }

  const url = req.url;

  if (url === "/v1/chat/completions" && req.method === "POST") {
    await handleChatCompletions(req, res);
  } else if (url === "/v1/models" && req.method === "GET") {
    handleModels(req, res);
  } else if (url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok" }));
  } else {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: { message: "Not found" } }));
  }
});

server.listen(PORT, () => {
  console.log(`Noah AI → OpenAI Proxy running on http://localhost:${PORT}`);
  console.log(`Endpoint: POST http://localhost:${PORT}/v1/chat/completions`);
  console.log(`Models:   GET  http://localhost:${PORT}/v1/models`);
  console.log(`\nUsage with OpenAI SDK:`);
  console.log(`  const client = new OpenAI({`);
  console.log(`    baseURL: "http://localhost:${PORT}/v1",`);
  console.log(`    apiKey: "YOUR_NOAH_AI_AUTH_TOKEN",`);
  console.log(`  });`);
});
