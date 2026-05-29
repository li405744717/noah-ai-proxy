/**
 * noah-ai-proxy — OpenAI-compatible bridge via ai.noahgroup.com (compliant path)
 *
 * Converts OpenAI /v1/chat/completions format ↔ Noah AI streamChat API.
 * Runs on your local machine, communicates only with the approved public domain.
 */

const http = require("http");
const https = require("https");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "4000", 10);
const HOST = process.env.HOST || "127.0.0.1";
const NOAH_BASE = process.env.NOAH_BASE || "https://ai.noahgroup.com";
const DEFAULT_MODEL = "gpt-5.5";
const DEFAULT_GPTS_ID = 76;

// ─── Utility ────────────────────────────────────────────────────────────────

function uuid() { return crypto.randomUUID(); }
function shortId() { return crypto.randomUUID().replace(/-/g, "").slice(0, 24); }

function parseJwt(token) {
  try {
    return JSON.parse(Buffer.from(token.split(".")[1], "base64").toString());
  } catch { return {}; }
}

function httpsReq(url, opts, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const reqOpts = {
      hostname: u.hostname,
      port: 443,
      path: u.pathname + u.search,
      method: opts.method || "POST",
      headers: opts.headers || {},
    };
    const req = https.request(reqOpts, (res) => {
      if (opts.stream) { resolve(res); return; }
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on("error", reject);
    if (body) req.write(typeof body === "string" ? body : JSON.stringify(body));
    req.end();
  });
}

// ─── Noah AI API ────────────────────────────────────────────────────────────

function buildHeaders(authToken, extraCookies) {
  return {
    "Content-Type": "application/json",
    Cookie: `NOAH_AI_AUTH_TOKEN=${authToken}${extraCookies ? "; " + extraCookies : ""}`,
    Origin: NOAH_BASE,
    Referer: `${NOAH_BASE}/home`,
    "User-Agent": "Mozilla/5.0 (compatible; noah-ai-proxy/2.0)",
    "request-id": uuid(),
  };
}

async function createSession(authToken, gptsId, extraCookies) {
  const res = await httpsReq(
    `${NOAH_BASE}/api/noah-chat-svc/session/createSession`,
    { method: "POST", headers: buildHeaders(authToken, extraCookies) },
    { gptsId, sessionName: "proxy-session" }
  );
  if (res.status === 200 && res.body?.code === 200 && res.body.data) {
    return res.body.data.sessionId || res.body.data.id || res.body.data;
  }
  throw new Error(`createSession failed: ${JSON.stringify(res.body).slice(0, 200)}`);
}

async function callStreamChat(authToken, sessionId, content, model, extraCookies) {
  const tokenInfo = parseJwt(authToken);
  const workNo = tokenInfo.workNo || tokenInfo.userUid || "proxy";

  return httpsReq(
    `${NOAH_BASE}/api/noah-chat-svc/chat/streamChat`,
    { method: "POST", headers: buildHeaders(authToken, extraCookies), stream: true },
    {
      sessionId,
      chatType: 0,
      content,
      currentGptsId: "",
      assistantType: 0,
      model: model || DEFAULT_MODEL,
      pluginIds: [2, 3],
      fileIds: [],
      chatRequestId: `${workNo}${Date.now()}${Math.floor(Math.random() * 1000)}`,
    }
  );
}

// ─── Noah SSE Parser ────────────────────────────────────────────────────────

class NoahSSEParser {
  constructor({ onText, onDone, onError }) {
    this.buffer = "";
    this.onText = onText;
    this.onDone = onDone;
    this.onError = onError;
  }

  feed(raw) {
    this.buffer += raw;
    const blocks = this.buffer.split("\n\n");
    this.buffer = blocks.pop() || "";
    for (const block of blocks) this._parse(block.trim());
  }

  flush() {
    if (this.buffer.trim()) { this._parse(this.buffer.trim()); this.buffer = ""; }
  }

  _parse(block) {
    if (!block) return;
    let eventType = "", dataLine = "";
    for (const line of block.split("\n")) {
      if (line.startsWith("event:")) eventType = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLine = line.slice(5).trim();
    }
    if (eventType === "end" || eventType === "stop" || eventType === "done" || dataLine === "[DONE]") {
      this.onDone(); return;
    }
    if (eventType === "error") { this.onError(new Error(dataLine || "upstream error")); return; }
    if (!dataLine) return;
    try {
      const d = JSON.parse(dataLine);
      const text = d.chunk || d.content || d.data?.content || "";
      if (text) this.onText(text);
    } catch {}
  }
}

// ─── OpenAI Format Builders ─────────────────────────────────────────────────

function openaiChunk(id, model, content, finishReason) {
  return {
    id: `chatcmpl-${id}`,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      delta: content !== null ? { content } : {},
      finish_reason: finishReason || null,
    }],
  };
}

function openaiToolCallChunks(id, model, toolCalls) {
  const chunks = [];
  for (let i = 0; i < toolCalls.length; i++) {
    const tc = toolCalls[i];
    chunks.push({
      id: `chatcmpl-${id}`,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [{
        index: 0,
        delta: { tool_calls: [{ index: i, id: tc.id, type: "function", function: { name: tc.function.name, arguments: "" } }] },
        finish_reason: null,
      }],
    });
    chunks.push({
      id: `chatcmpl-${id}`,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [{
        index: 0,
        delta: { tool_calls: [{ index: i, function: { arguments: tc.function.arguments } }] },
        finish_reason: null,
      }],
    });
  }
  return chunks;
}

function openaiResponse(id, model, content, toolCalls) {
  const message = { role: "assistant" };
  if (toolCalls && toolCalls.length > 0) {
    message.content = content || null;
    message.tool_calls = toolCalls;
  } else {
    message.content = content;
  }
  return {
    id: `chatcmpl-${id}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      message,
      finish_reason: toolCalls?.length > 0 ? "tool_calls" : "stop",
    }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}

// ─── Tool Calling Support ───────────────────────────────────────────────────

const TOOL_SYSTEM_PROMPT = `You are operating in function-calling mode. When the user's request matches available functions, respond ONLY with tool calls in this format:

<tool_call>
{"name": "function_name", "arguments": {"param1": "value1"}}
</tool_call>

Rules:
- If a function matches, output ONLY <tool_call> blocks, nothing else.
- Multiple calls: output multiple <tool_call> blocks.
- If NO function matches, respond normally in plain text.

Available functions:
`;

function buildToolSystemPrompt(tools) {
  if (!tools || !tools.length) return "";
  let prompt = TOOL_SYSTEM_PROMPT;
  for (const tool of tools) {
    const fn = tool.function || tool;
    prompt += `\n### ${fn.name}\n`;
    if (fn.description) prompt += `${fn.description}\n`;
    if (fn.parameters) prompt += `Parameters: ${JSON.stringify(fn.parameters)}\n`;
  }
  return prompt;
}

function parseToolCallsFromText(text) {
  const calls = [];
  const re = /<tool_call>\s*([\s\S]*?)\s*<\/tool_call>/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    try {
      const parsed = JSON.parse(m[1].trim());
      calls.push({
        id: `call_${shortId()}`,
        type: "function",
        function: {
          name: parsed.name,
          arguments: typeof parsed.arguments === "string" ? parsed.arguments : JSON.stringify(parsed.arguments),
        },
      });
    } catch {}
  }
  return calls;
}

function stripToolCalls(text) {
  return text.replace(/<tool_call>\s*[\s\S]*?\s*<\/tool_call>/g, "").trim();
}

// ─── Message Formatting ─────────────────────────────────────────────────────

function formatMessages(messages, tools) {
  const parts = [];

  if (tools && tools.length > 0) {
    parts.push(buildToolSystemPrompt(tools));
  }

  for (const msg of messages) {
    const role = msg.role;
    if (role === "system") {
      parts.push(`[System]\n${extractText(msg)}`);
    } else if (role === "user") {
      parts.push(`[User]\n${extractText(msg)}`);
    } else if (role === "assistant") {
      if (msg.content) parts.push(`[Assistant]\n${msg.content}`);
      if (msg.tool_calls) {
        for (const tc of msg.tool_calls) {
          parts.push(`[Assistant Tool Call]\n<tool_call>\n{"name":"${tc.function.name}","arguments":${tc.function.arguments}}\n</tool_call>`);
        }
      }
    } else if (role === "tool") {
      parts.push(`[Tool Result (${msg.name || msg.tool_call_id || "?"})]\n${msg.content}`);
    }
  }
  return parts.join("\n\n");
}

function extractText(msg) {
  if (typeof msg.content === "string") return msg.content;
  if (Array.isArray(msg.content)) {
    return msg.content.filter((p) => p.type === "text").map((p) => p.text).join("\n");
  }
  return String(msg.content || "");
}

// ─── Request Handler ────────────────────────────────────────────────────────

async function handleCompletions(reqBody, authToken, extraCookies, res) {
  const model = reqBody.model || DEFAULT_MODEL;
  const stream = reqBody.stream || false;
  const messages = reqBody.messages || [];
  const tools = reqBody.tools || [];
  const hasTools = tools.length > 0;
  const gptsId = reqBody.gpts_id || DEFAULT_GPTS_ID;

  const content = formatMessages(messages, tools);
  if (!content) {
    sendJson(res, 400, { error: { message: "No message content", type: "invalid_request_error" } });
    return;
  }

  const completionId = shortId();

  const sessionId = await createSession(authToken, gptsId, extraCookies);
  const upstream = await callStreamChat(authToken, sessionId, content, model, extraCookies);

  if (stream && !hasTools) {
    // Pure streaming — no tool calling
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "X-Accel-Buffering": "no",
    });

    // Initial role chunk
    writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

    const parser = new NoahSSEParser({
      onText: (text) => { if (!res.writableEnded) writeSse(res, openaiChunk(completionId, model, text, null)); },
      onDone: () => { finishStream(res, completionId, model); },
      onError: (err) => {
        if (!res.writableEnded) {
          writeSse(res, { error: { message: err.message } });
          res.write("data: [DONE]\n\n");
          res.end();
        }
      },
    });

    upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
    upstream.on("end", () => { parser.flush(); finishStream(res, completionId, model); });
    upstream.on("error", (err) => { if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.end(); } });

  } else {
    // Collect full response (needed for tool call detection or non-stream)
    const fullText = await collectResponse(upstream);

    let toolCalls = [];
    let responseContent = fullText;

    if (hasTools) {
      toolCalls = parseToolCallsFromText(fullText);
      if (toolCalls.length > 0) {
        responseContent = stripToolCalls(fullText) || null;
      }
    }

    if (stream) {
      // Stream mode with tool calls — simulate streaming after collection
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "Access-Control-Allow-Origin": "*",
        "X-Accel-Buffering": "no",
      });

      writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

      if (toolCalls.length > 0) {
        for (const chunk of openaiToolCallChunks(completionId, model, toolCalls)) {
          writeSse(res, chunk);
        }
        writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] });
      } else {
        if (responseContent) writeSse(res, openaiChunk(completionId, model, responseContent, null));
        writeSse(res, openaiChunk(completionId, model, null, "stop"));
      }
      res.write("data: [DONE]\n\n");
      res.end();

    } else {
      // Non-stream
      sendJson(res, 200, openaiResponse(completionId, model, responseContent, toolCalls.length > 0 ? toolCalls : null));
    }
  }
}

function collectResponse(upstream) {
  return new Promise((resolve, reject) => {
    let full = "";
    const parser = new NoahSSEParser({
      onText: (text) => { full += text; },
      onDone: () => { resolve(full); },
      onError: (err) => { reject(err); },
    });
    upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
    upstream.on("end", () => { parser.flush(); resolve(full); });
    upstream.on("error", reject);
  });
}

function finishStream(res, id, model) {
  if (res.writableEnded) return;
  writeSse(res, openaiChunk(id, model, null, "stop"));
  res.write("data: [DONE]\n\n");
  res.end();
}

function writeSse(res, data) {
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify(data));
}

// ─── HTTP Server ────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-auth-token, x-extra-cookies",
    });
    res.end();
    return;
  }

  const url = req.url.split("?")[0];

  if (url === "/v1/chat/completions" && req.method === "POST") {
    // Parse body
    let body = "";
    await new Promise((r) => { req.on("data", (c) => (body += c)); req.on("end", r); });

    let params;
    try { params = JSON.parse(body); }
    catch { sendJson(res, 400, { error: { message: "Invalid JSON", type: "invalid_request_error" } }); return; }

    // Extract auth
    const authHeader = req.headers["authorization"] || "";
    const authToken = req.headers["x-auth-token"] || (authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "") || process.env.NOAH_AUTH_TOKEN || "";

    if (!authToken) {
      sendJson(res, 401, { error: { message: "Missing auth token. Use Authorization: Bearer <NOAH_AI_AUTH_TOKEN>", type: "authentication_error" } });
      return;
    }

    const extraCookies = req.headers["x-extra-cookies"] || params.extra_cookies || process.env.NOAH_EXTRA_COOKIES || "";

    try {
      await handleCompletions(params, authToken, extraCookies, res);
    } catch (err) {
      if (!res.headersSent) sendJson(res, 500, { error: { message: err.message, type: "server_error" } });
      else if (!res.writableEnded) { res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`); res.end(); }
    }

  } else if (url === "/v1/models" && req.method === "GET") {
    sendJson(res, 200, {
      object: "list",
      data: [
        { id: "gpt-5.5", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gpt-5.5-thinking", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gpt-4o", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gpt-4o-mini", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "deepseek-r1", object: "model", created: 1700000000, owned_by: "noah-ai" },
      ],
    });

  } else if (url === "/health") {
    sendJson(res, 200, { status: "ok", version: "2.0.0", upstream: NOAH_BASE });

  } else {
    sendJson(res, 404, { error: { message: "Not found", type: "invalid_request_error" } });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`noah-ai-proxy v2.0 — OpenAI-compatible bridge`);
  console.log(`Listening: http://${HOST}:${PORT}`);
  console.log(`Upstream:  ${NOAH_BASE}`);
  console.log(`\nEndpoints:`);
  console.log(`  POST /v1/chat/completions  — Chat (stream + non-stream + tool calling)`);
  console.log(`  GET  /v1/models            — Model list`);
  console.log(`  GET  /health               — Health check`);
});

process.on("SIGTERM", () => { server.close(); process.exit(0); });
process.on("SIGINT", () => { server.close(); process.exit(0); });
