/**
 * noah-ai-proxy v2.1 — OpenAI-compatible bridge via ai.noahgroup.com
 *
 * Features:
 * - Fixed session pool (10 sessions, created at startup, reused across requests)
 * - Concurrent request handling with pool-based session allocation
 * - Full OpenAI Chat Completions API compatibility
 */

const http = require("http");
const https = require("https");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const PORT = parseInt(process.env.PORT || "4000", 10);
const HOST = process.env.HOST || "127.0.0.1";
const NOAH_BASE = process.env.NOAH_BASE || "https://ai.noahgroup.com";
const DEFAULT_MODEL = "gpt-5.5";
const DEFAULT_GPTS_ID = parseInt(process.env.GPTS_ID || "87", 10);
const DEFAULT_SKILL_NAME = process.env.SKILL_NAME || "tool-skills-creator";
const POOL_SIZE = parseInt(process.env.POOL_SIZE || "10", 10);

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

// ─── Session Pool ───────────────────────────────────────────────────────────

const POOL_FILE = path.join(__dirname, ".sessions.json");

class SessionPool {
  constructor(size) {
    this.size = size;
    this.sessions = [];       // [{id, busy, createdAt, useCount}]
    this.waitQueue = [];      // callbacks waiting for a free session
    this.initialized = false;
  }

  async init(authToken, extraCookies, gptsId) {
    // Step 1: Fetch existing proxy sessions from server (matching gptsId)
    const existing = await this._fetchExistingSessions(authToken, extraCookies, gptsId);
    console.log(`[Pool] Found ${existing.length} existing proxy-pool sessions on server`);

    // Step 2: Use existing sessions (up to pool size)
    for (const id of existing.slice(0, this.size)) {
      this.sessions.push({ id, busy: false, createdAt: Date.now(), useCount: 0 });
      console.log(`[Pool] Reusing session: ${id}`);
    }

    // Step 3: Create missing sessions to fill the pool
    const needed = this.size - this.sessions.length;
    if (needed > 0) {
      console.log(`[Pool] Creating ${needed} new sessions...`);
      const results = await Promise.allSettled(
        Array.from({ length: needed }, (_, i) =>
          this._createSession(authToken, extraCookies, gptsId, this.sessions.length + i)
        )
      );
      for (const r of results) {
        if (r.status === "fulfilled") {
          this.sessions.push({ id: r.value, busy: false, createdAt: Date.now(), useCount: 0 });
          console.log(`[Pool] Created new session: ${r.value}`);
        } else {
          console.error(`[Pool] Create failed: ${r.reason?.message}`);
        }
      }
    }

    // Step 4: Persist to local file as fallback
    this._saveToDisk();

    this.initialized = true;
    console.log(`[Pool] Ready: ${this.sessions.length}/${this.size} sessions`);

    if (this.sessions.length === 0) {
      throw new Error("Failed to create any sessions");
    }
  }

  async _fetchExistingSessions(authToken, extraCookies, gptsId) {
    try {
      const headers = buildHeaders(authToken, extraCookies);
      const res = await httpsReq(
        `${NOAH_BASE}/api/noah-chat-svc/session/listSessionRecord?pageNo=1&pageSize=50`,
        { method: "GET", headers },
        null
      );
      if (res.status === 200 && res.body?.code === 200 && Array.isArray(res.body.data)) {
        return res.body.data
          .filter((r) => r.name && r.name.startsWith("proxy-pool-") && r.gptsId === gptsId)
          .map((r) => r.id)
          .filter(Boolean);
      }
      return this._loadFromDisk();
    } catch (err) {
      console.warn(`[Pool] Failed to fetch sessions from server: ${err.message}`);
      return this._loadFromDisk();
    }
  }

  _loadFromDisk() {
    try {
      if (fs.existsSync(POOL_FILE)) {
        const data = JSON.parse(fs.readFileSync(POOL_FILE, "utf-8"));
        if (Array.isArray(data.sessionIds)) {
          console.log(`[Pool] Loaded ${data.sessionIds.length} sessions from local fallback`);
          return data.sessionIds;
        }
      }
    } catch (err) {
      console.warn(`[Pool] Failed to read ${POOL_FILE}: ${err.message}`);
    }
    return [];
  }

  _saveToDisk() {
    try {
      const data = { sessionIds: this.sessions.map((s) => s.id), updatedAt: new Date().toISOString() };
      fs.writeFileSync(POOL_FILE, JSON.stringify(data, null, 2), "utf-8");
    } catch (err) {
      console.warn(`[Pool] Failed to write ${POOL_FILE}: ${err.message}`);
    }
  }

  acquire() {
    const free = this.sessions.find((s) => !s.busy);
    if (free) {
      free.busy = true;
      free.useCount++;
      return Promise.resolve(free);
    }
    // All busy — queue the request
    return new Promise((resolve) => {
      this.waitQueue.push(resolve);
    });
  }

  release(session) {
    session.busy = false;
    // If someone is waiting, give them this session immediately
    if (this.waitQueue.length > 0) {
      const next = this.waitQueue.shift();
      session.busy = true;
      session.useCount++;
      next(session);
    }
  }

  stats() {
    const busy = this.sessions.filter((s) => s.busy).length;
    return {
      total: this.sessions.length,
      busy,
      free: this.sessions.length - busy,
      waiting: this.waitQueue.length,
    };
  }

  async _createSession(authToken, extraCookies, gptsId, index) {
    const headers = buildHeaders(authToken, extraCookies);
    const body = { gptsId, sessionName: `proxy-pool-${index}` };
    // gptsId 87 = Claude SDK with workspace (supports tool call + file generation)
    if (gptsId === 87) {
      body.skillName = DEFAULT_SKILL_NAME;
    }
    const res = await httpsReq(
      `${NOAH_BASE}/api/noah-chat-svc/session/createSession`,
      { method: "POST", headers },
      body
    );
    if (res.status === 200 && res.body?.code === 200 && res.body.data) {
      return res.body.data.sessionId || res.body.data.id || res.body.data;
    }
    throw new Error(`createSession failed: ${JSON.stringify(res.body).slice(0, 200)}`);
  }
}

const pool = new SessionPool(POOL_SIZE);

// ─── Noah AI API ────────────────────────────────────────────────────────────

function buildHeaders(authToken, extraCookies) {
  return {
    "Content-Type": "application/json",
    Cookie: `NOAH_AI_AUTH_TOKEN=${authToken}${extraCookies ? "; " + extraCookies : ""}`,
    Origin: NOAH_BASE,
    Referer: `${NOAH_BASE}/home`,
    "User-Agent": "Mozilla/5.0 (compatible; noah-ai-proxy/2.1)",
    "request-id": uuid(),
  };
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
    this.done = false;
  }

  feed(raw) {
    if (this.done) return;
    this.buffer += raw;
    const blocks = this.buffer.split("\n\n");
    this.buffer = blocks.pop() || "";
    for (const block of blocks) this._parse(block.trim());
  }

  flush() {
    if (this.done) return;
    if (this.buffer.trim()) { this._parse(this.buffer.trim()); this.buffer = ""; }
  }

  _parse(block) {
    if (!block || this.done) return;
    let eventType = "", dataLine = "";
    for (const line of block.split("\n")) {
      if (line.startsWith("event:")) eventType = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLine = line.slice(5).trim();
    }
    if (eventType === "end" || eventType === "stop" || eventType === "done" || dataLine === "[DONE]") {
      this.done = true; this.onDone(); return;
    }
    if (eventType === "error") { this.done = true; this.onError(new Error(dataLine || "upstream error")); return; }
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
      id: `chatcmpl-${id}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model,
      choices: [{ index: 0, delta: { tool_calls: [{ index: i, id: tc.id, type: "function", function: { name: tc.function.name, arguments: "" } }] }, finish_reason: null }],
    });
    chunks.push({
      id: `chatcmpl-${id}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model,
      choices: [{ index: 0, delta: { tool_calls: [{ index: i, function: { arguments: tc.function.arguments } }] }, finish_reason: null }],
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
    id: `chatcmpl-${id}`, object: "chat.completion", created: Math.floor(Date.now() / 1000), model,
    choices: [{ index: 0, message, finish_reason: toolCalls?.length > 0 ? "tool_calls" : "stop" }],
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
  if (tools && tools.length > 0) parts.push(buildToolSystemPrompt(tools));

  for (const msg of messages) {
    if (msg.role === "system") parts.push(`[System]\n${extractText(msg)}`);
    else if (msg.role === "user") parts.push(`[User]\n${extractText(msg)}`);
    else if (msg.role === "assistant") {
      if (msg.content) parts.push(`[Assistant]\n${msg.content}`);
      if (msg.tool_calls) {
        for (const tc of msg.tool_calls) {
          parts.push(`[Assistant Tool Call]\n<tool_call>\n{"name":"${tc.function.name}","arguments":${tc.function.arguments}}\n</tool_call>`);
        }
      }
    } else if (msg.role === "tool") {
      parts.push(`[Tool Result (${msg.name || msg.tool_call_id || "?"})]\n${msg.content}`);
    }
  }
  return parts.join("\n\n");
}

function extractText(msg) {
  if (typeof msg.content === "string") return msg.content;
  if (Array.isArray(msg.content)) return msg.content.filter((p) => p.type === "text").map((p) => p.text).join("\n");
  return String(msg.content || "");
}

// ─── Request Handler ────────────────────────────────────────────────────────

async function handleCompletions(reqBody, authToken, extraCookies, res) {
  const model = reqBody.model || DEFAULT_MODEL;
  const stream = reqBody.stream || false;
  const messages = reqBody.messages || [];
  const tools = reqBody.tools || [];
  const hasTools = tools.length > 0;

  const content = formatMessages(messages, tools);
  if (!content) { sendJson(res, 400, { error: { message: "No message content", type: "invalid_request_error" } }); return; }

  const completionId = shortId();

  // Acquire session from pool
  const session = await pool.acquire();

  try {
    const upstream = await callStreamChat(authToken, session.id, content, model, extraCookies);

    if (stream && !hasTools) {
      res.writeHead(200, {
        "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
        Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no",
      });
      writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

      const parser = new NoahSSEParser({
        onText: (text) => { if (!res.writableEnded) writeSse(res, openaiChunk(completionId, model, text, null)); },
        onDone: () => { pool.release(session); finishStream(res, completionId, model); },
        onError: (err) => { pool.release(session); if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.write("data: [DONE]\n\n"); res.end(); } },
      });

      upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
      upstream.on("end", () => { parser.flush(); if (!parser.done) { pool.release(session); finishStream(res, completionId, model); } });
      upstream.on("error", (err) => { pool.release(session); if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.end(); } });

    } else {
      const fullText = await collectResponse(upstream);
      pool.release(session);

      let toolCalls = [];
      let responseContent = fullText;
      if (hasTools) {
        toolCalls = parseToolCallsFromText(fullText);
        if (toolCalls.length > 0) responseContent = stripToolCalls(fullText) || null;
      }

      if (stream) {
        res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no" });
        writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

        if (toolCalls.length > 0) {
          for (const chunk of openaiToolCallChunks(completionId, model, toolCalls)) writeSse(res, chunk);
          writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] });
        } else {
          if (responseContent) writeSse(res, openaiChunk(completionId, model, responseContent, null));
          writeSse(res, openaiChunk(completionId, model, null, "stop"));
        }
        res.write("data: [DONE]\n\n"); res.end();
      } else {
        sendJson(res, 200, openaiResponse(completionId, model, responseContent, toolCalls.length > 0 ? toolCalls : null));
      }
    }
  } catch (err) {
    pool.release(session);
    if (!res.headersSent) sendJson(res, 500, { error: { message: err.message, type: "server_error" } });
    else if (!res.writableEnded) { res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`); res.end(); }
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

function writeSse(res, data) { res.write(`data: ${JSON.stringify(data)}\n\n`); }
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify(data));
}

// ─── HTTP Server ────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, Authorization, x-auth-token, x-extra-cookies" });
    res.end(); return;
  }

  const url = req.url.split("?")[0];

  if (url === "/v1/chat/completions" && req.method === "POST") {
    let body = "";
    await new Promise((r) => { req.on("data", (c) => (body += c)); req.on("end", r); });

    let params;
    try { params = JSON.parse(body); }
    catch { sendJson(res, 400, { error: { message: "Invalid JSON", type: "invalid_request_error" } }); return; }

    const authHeader = req.headers["authorization"] || "";
    const authToken = req.headers["x-auth-token"] || (authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "") || process.env.NOAH_AUTH_TOKEN || "";
    if (!authToken) { sendJson(res, 401, { error: { message: "Missing auth token", type: "authentication_error" } }); return; }

    const extraCookies = req.headers["x-extra-cookies"] || params.extra_cookies || process.env.NOAH_EXTRA_COOKIES || "";

    if (!pool.initialized) { sendJson(res, 503, { error: { message: "Session pool not ready", type: "server_error" } }); return; }

    try { await handleCompletions(params, authToken, extraCookies, res); }
    catch (err) {
      if (!res.headersSent) sendJson(res, 500, { error: { message: err.message, type: "server_error" } });
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

  } else if (url === "/health" || url === "/pool") {
    sendJson(res, 200, { status: "ok", version: "2.1.0", upstream: NOAH_BASE, pool: pool.stats() });

  } else {
    sendJson(res, 404, { error: { message: "Not found", type: "invalid_request_error" } });
  }
});

// ─── Startup ────────────────────────────────────────────────────────────────

async function main() {
  const authToken = process.env.NOAH_AUTH_TOKEN;
  const extraCookies = process.env.NOAH_EXTRA_COOKIES || "";

  if (!authToken) {
    console.error("[Fatal] NOAH_AUTH_TOKEN environment variable is required");
    process.exit(1);
  }

  try {
    await pool.init(authToken, extraCookies, DEFAULT_GPTS_ID);
  } catch (err) {
    console.error(`[Fatal] Pool init failed: ${err.message}`);
    process.exit(1);
  }

  server.listen(PORT, HOST, () => {
    console.log(`\nnoah-ai-proxy v2.1 — Session Pool Mode`);
    console.log(`Listening: http://${HOST}:${PORT}`);
    console.log(`Upstream:  ${NOAH_BASE}`);
    console.log(`Pool:      ${pool.sessions.length} sessions ready`);
    console.log(`\nEndpoints:`);
    console.log(`  POST /v1/chat/completions`);
    console.log(`  GET  /v1/models`);
    console.log(`  GET  /health (includes pool stats)`);
  });
}

main();

process.on("SIGTERM", () => { server.close(); process.exit(0); });
process.on("SIGINT", () => { server.close(); process.exit(0); });
