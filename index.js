/**
 * noah-ai-proxy v4.0 — OpenAI-compatible bridge via ai.noahgroup.com
 *
 * v4 Architecture: Incremental messaging with session affinity
 * - Each agent conversation maps to a dedicated Noah session
 * - Only NEW messages are sent each turn (Noah session retains context)
 * - Tool schema injected once on first turn, not repeated
 * - Reset detection: if conversation diverges, auto-create new session
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
const DEFAULT_GPTS_ID = parseInt(process.env.GPTS_ID || "76", 10);

// ─── Utility ────────────────────────────────────────────────────────────────

function uuid() { return crypto.randomUUID(); }
function shortId() { return crypto.randomUUID().replace(/-/g, "").slice(0, 24); }

function parseJwt(token) {
  try {
    return JSON.parse(Buffer.from(token.split(".")[1], "base64").toString());
  } catch { return {}; }
}

function md5(str) { return crypto.createHash("md5").update(str).digest("hex"); }

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

// ─── Conversation Session Manager ──────────────────────────────────────────
// Maps agent conversations to Noah sessions, tracks message state

const CONV_FILE = path.join(__dirname, ".conversations.json");

class ConversationManager {
  constructor() {
    // convId → { noahSessionId, sentCount, fingerprint, createdAt, lastUsed, busy }
    this.conversations = new Map();
    this.waitQueues = new Map(); // convId → [resolve callbacks]
    this._load();
  }

  _load() {
    try {
      if (fs.existsSync(CONV_FILE)) {
        const data = JSON.parse(fs.readFileSync(CONV_FILE, "utf-8"));
        if (data.conversations) {
          for (const [k, v] of Object.entries(data.conversations)) {
            this.conversations.set(k, { ...v, busy: false });
          }
          console.log(`[ConvMgr] Loaded ${this.conversations.size} conversation mappings`);
        }
      }
    } catch (err) {
      console.warn(`[ConvMgr] Failed to load: ${err.message}`);
    }
  }

  _save() {
    try {
      const obj = {};
      for (const [k, v] of this.conversations) {
        obj[k] = { noahSessionId: v.noahSessionId, sentCount: v.sentCount, fingerprint: v.fingerprint, createdAt: v.createdAt, lastUsed: v.lastUsed };
      }
      fs.writeFileSync(CONV_FILE, JSON.stringify({ conversations: obj, updatedAt: new Date().toISOString() }, null, 2), "utf-8");
    } catch {}
  }

  getConversationId(messages) {
    // Use system message + first user message as conversation identity
    const systemMsg = messages.find(m => m.role === "system");
    const firstUser = messages.find(m => m.role === "user");
    const key = (systemMsg ? extractText(systemMsg) : "") + "||" + (firstUser ? extractText(firstUser).slice(0, 100) : "");
    return md5(key);
  }

  getFingerprint(messages) {
    // Fingerprint = hash of first 3 messages to detect conversation reset
    const first3 = messages.slice(0, 3).map(m => `${m.role}:${extractText(m).slice(0, 50)}`).join("|");
    return md5(first3);
  }

  async acquire(convId, messages, authToken, extraCookies) {
    const existing = this.conversations.get(convId);
    const fingerprint = this.getFingerprint(messages);

    if (existing && existing.fingerprint === fingerprint && !existing.busy) {
      existing.busy = true;
      existing.lastUsed = Date.now();
      return existing;
    }

    if (existing && existing.fingerprint === fingerprint && existing.busy) {
      // Same conversation, but busy — wait
      return new Promise((resolve) => {
        if (!this.waitQueues.has(convId)) this.waitQueues.set(convId, []);
        this.waitQueues.get(convId).push(resolve);
      });
    }

    // New conversation or fingerprint changed (conversation reset)
    if (existing && existing.fingerprint !== fingerprint) {
      console.log(`[ConvMgr] Conversation ${convId.slice(0,8)} reset detected, creating new session`);
    }

    // Create new Noah session for this conversation
    const noahSessionId = await this._createSession(authToken, extraCookies, convId);
    const conv = {
      noahSessionId,
      sentCount: 0,
      fingerprint,
      createdAt: Date.now(),
      lastUsed: Date.now(),
      busy: true,
    };
    this.conversations.set(convId, conv);
    this._save();
    return conv;
  }

  release(convId) {
    const conv = this.conversations.get(convId);
    if (!conv) return;
    conv.busy = false;
    this._save();

    // Wake up waiting requests for this conversation
    const queue = this.waitQueues.get(convId);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      conv.busy = true;
      next(conv);
    }
  }

  updateSentCount(convId, count) {
    const conv = this.conversations.get(convId);
    if (conv) {
      conv.sentCount = count;
      this._save();
    }
  }

  async _createSession(authToken, extraCookies, convId) {
    const headers = buildHeaders(authToken, extraCookies);
    const body = { gptsId: DEFAULT_GPTS_ID, sessionName: `conv-${convId.slice(0, 8)}` };
    const res = await httpsReq(
      `${NOAH_BASE}/api/noah-chat-svc/session/createSession`,
      { method: "POST", headers },
      body
    );
    if (res.status === 200 && res.body?.code === 200 && res.body.data) {
      const id = res.body.data.sessionId || res.body.data.id || res.body.data;
      console.log(`[ConvMgr] Created session ${id} for conv ${convId.slice(0,8)}`);
      return id;
    }
    throw new Error(`createSession failed: ${JSON.stringify(res.body).slice(0, 200)}`);
  }

  stats() {
    let busy = 0;
    for (const v of this.conversations.values()) { if (v.busy) busy++; }
    return { total: this.conversations.size, busy, free: this.conversations.size - busy };
  }
}

const convMgr = new ConversationManager();

// ─── Noah AI API ────────────────────────────────────────────────────────────

function buildHeaders(authToken, extraCookies) {
  return {
    "Content-Type": "application/json",
    Cookie: `NOAH_AI_AUTH_TOKEN=${authToken}${extraCookies ? "; " + extraCookies : ""}`,
    Origin: NOAH_BASE,
    Referer: `${NOAH_BASE}/home`,
    "User-Agent": "Mozilla/5.0 (compatible; noah-ai-proxy/4.0)",
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

const TOOL_SYSTEM_PROMPT = `[CRITICAL INSTRUCTION]

You are an API backend in STRICT function-calling mode. You MUST NOT execute any action yourself. You MUST NOT write files, run code, or describe steps. Your ONLY job: analyze the request and return structured tool_call commands.

Output format (respond with ONLY this, nothing else):

<tool_call>
{"name": "function_name", "arguments": {"param1": "value1"}}
</tool_call>

Rules:
- Output ONLY <tool_call> blocks. No text, no explanations, no markdown.
- Multiple calls = multiple <tool_call> blocks.
- If NO function matches, respond in plain text.
- NEVER execute the action. ONLY return the command.

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

// ─── Incremental Message Formatting ────────────────────────────────────────

function extractText(msg) {
  if (typeof msg.content === "string") return msg.content;
  if (Array.isArray(msg.content)) return msg.content.filter((p) => p.type === "text").map((p) => p.text).join("\n");
  return String(msg.content || "");
}

function formatIncrementalMessages(messages, tools, sentCount) {
  // First turn: send tool schema + all messages
  // Subsequent turns: only send new messages since last sentCount
  const isFirstTurn = sentCount === 0;
  const newMessages = messages.slice(sentCount);

  if (newMessages.length === 0) {
    return { content: null, newSentCount: sentCount };
  }

  const parts = [];
  const hasTools = tools && tools.length > 0;

  // Only inject tool schema on first turn
  if (isFirstTurn && hasTools) {
    parts.push(buildToolSystemPrompt(tools));
  }

  for (const msg of newMessages) {
    if (msg.role === "system") {
      if (isFirstTurn) parts.push(`[System]\n${extractText(msg)}`);
      // Skip system messages on subsequent turns (already in session context)
    } else if (msg.role === "user") {
      parts.push(`[User]\n${extractText(msg)}`);
    } else if (msg.role === "assistant") {
      if (msg.content) parts.push(`[Assistant]\n${typeof msg.content === 'string' ? msg.content : extractText(msg)}`);
      if (msg.tool_calls) {
        for (const tc of msg.tool_calls) {
          parts.push(`[Assistant Tool Call]\n<tool_call>\n{"name":"${tc.function.name}","arguments":${tc.function.arguments}}\n</tool_call>`);
        }
      }
    } else if (msg.role === "tool") {
      parts.push(`[Tool Result (${msg.name || msg.tool_call_id || "?"})]\n${msg.content}`);
    }
  }

  // Add reminder based on context
  if (hasTools) {
    const lastMsg = messages[messages.length - 1];
    if (lastMsg && lastMsg.role === "tool") {
      parts.push(`[Reminder] The tool has finished executing. Based on the result above, decide your next step:
- If more actions are needed to fulfill the user's original request, output additional <tool_call> blocks.
- If the task is complete, provide a brief text summary of what was accomplished.
Do NOT execute actions yourself. Only use <tool_call> blocks for actions.`);
    } else {
      parts.push(`[Reminder] Respond ONLY with <tool_call> blocks matching the available functions. Do NOT execute actions yourself. Output nothing else unless no function matches.`);
    }
  }

  return { content: parts.join("\n\n"), newSentCount: messages.length };
}

// ─── Request Handler ────────────────────────────────────────────────────────

async function handleCompletions(reqBody, authToken, extraCookies, res) {
  const model = reqBody.model || DEFAULT_MODEL;
  const stream = reqBody.stream || false;
  const messages = reqBody.messages || [];
  const tools = reqBody.tools || [];
  const hasTools = tools.length > 0;

  const completionId = shortId();
  const convId = convMgr.getConversationId(messages);

  // Acquire conversation session
  const conv = await convMgr.acquire(convId, messages, authToken, extraCookies);
  console.log(`[Req ${completionId}] conv=${convId.slice(0,8)} session=${conv.noahSessionId} sentCount=${conv.sentCount}→${messages.length} model=${model} hasTools=${hasTools}`);

  // Format only incremental messages
  const { content, newSentCount } = formatIncrementalMessages(messages, tools, conv.sentCount);

  if (!content) {
    convMgr.release(convId);
    sendJson(res, 400, { error: { message: "No new message content", type: "invalid_request_error" } });
    return;
  }

  const contentSize = Buffer.byteLength(content, 'utf-8');
  console.log(`[Req ${completionId}] sending ${contentSize} bytes (incremental: msgs ${conv.sentCount}→${newSentCount})`);

  try {
    const upstream = await callStreamChat(authToken, conv.noahSessionId, content, model, extraCookies);
    console.log(`[Req ${completionId}] upstream status=${upstream.statusCode}`);

    if (upstream.statusCode === 405 || upstream.statusCode >= 400) {
      const body = await collectRawBody(upstream);
      console.log(`[Req ${completionId}] upstream error body (first 200): ${body.slice(0, 200)}`);
      convMgr.release(convId);
      sendJson(res, 502, { error: { message: `Upstream returned ${upstream.statusCode}`, type: "upstream_error" } });
      return;
    }

    if (stream && !hasTools) {
      res.writeHead(200, {
        "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
        Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no",
      });
      writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

      const parser = new NoahSSEParser({
        onText: (text) => { if (!res.writableEnded) writeSse(res, openaiChunk(completionId, model, text, null)); },
        onDone: () => { convMgr.updateSentCount(convId, newSentCount); convMgr.release(convId); finishStream(res, completionId, model); },
        onError: (err) => { convMgr.release(convId); if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.write("data: [DONE]\n\n"); res.end(); } },
      });

      upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
      upstream.on("end", () => { parser.flush(); if (!parser.done) { convMgr.updateSentCount(convId, newSentCount); convMgr.release(convId); finishStream(res, completionId, model); } });
      upstream.on("error", (err) => { convMgr.release(convId); if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.end(); } });

    } else {
      const fullText = await collectResponse(upstream);
      console.log(`[Req ${completionId}] collected ${fullText.length} chars: ${fullText.slice(0, 200)}${fullText.length > 200 ? '...' : ''}`);

      // Update sent count on success
      convMgr.updateSentCount(convId, newSentCount);
      convMgr.release(convId);

      let toolCalls = [];
      let responseContent = fullText;
      if (hasTools) {
        toolCalls = parseToolCallsFromText(fullText);
        console.log(`[Req ${completionId}] parsed ${toolCalls.length} tool_calls`);
        if (toolCalls.length > 0) responseContent = stripToolCalls(fullText) || null;
      }

      if (stream) {
        res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no" });
        writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

        if (toolCalls.length > 0) {
          console.log(`[Req ${completionId}] streaming ${toolCalls.length} tool_calls to client`);
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
    convMgr.release(convId);
    console.error(`[Req ${completionId}] error: ${err.message}`);
    if (!res.headersSent) sendJson(res, 500, { error: { message: err.message, type: "server_error" } });
    else if (!res.writableEnded) { res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`); res.end(); }
  }
}

function collectResponse(upstream) {
  return new Promise((resolve, reject) => {
    let full = "";
    let resolved = false;
    let rawChunks = [];
    const done = (src) => {
      if (!resolved) {
        resolved = true;
        console.log(`[collectResponse] resolved via ${src}, length=${full.length}`);
        if (full.length === 0 && rawChunks.length > 0) {
          console.log(`[collectResponse] RAW upstream (first 500): ${rawChunks.join('').slice(0, 500)}`);
        }
        resolve(full);
      }
    };
    const parser = new NoahSSEParser({
      onText: (text) => { full += text; },
      onDone: () => { done("sse-done"); },
      onError: (err) => { if (!resolved) { resolved = true; reject(err); } },
    });
    upstream.on("data", (chunk) => { const s = chunk.toString("utf-8"); rawChunks.push(s); parser.feed(s); });
    upstream.on("end", () => {
      parser.flush();
      if (full.length === 0 && rawChunks.length > 0) {
        const raw = rawChunks.join('');
        try {
          const errBody = JSON.parse(raw);
          if (errBody.code && errBody.code !== 200) {
            if (!resolved) { resolved = true; reject(new Error(`upstream error: ${errBody.msg || errBody.message || raw.slice(0, 200)}`)); return; }
          }
        } catch {}
      }
      done("stream-end");
    });
    upstream.on("error", (err) => { if (!resolved) { resolved = true; reject(err); } });
  });
}

function collectRawBody(stream) {
  return new Promise((resolve) => {
    let data = "";
    stream.on("data", (c) => (data += c.toString("utf-8")));
    stream.on("end", () => resolve(data));
    stream.on("error", () => resolve(data));
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
  const reqTime = new Date().toLocaleTimeString();

  if (url === "/v1/chat/completions" && req.method === "POST") {
    let body = "";
    await new Promise((r) => { req.on("data", (c) => (body += c)); req.on("end", r); });

    let params;
    try { params = JSON.parse(body); }
    catch { console.log(`[${reqTime}] ← 400 Invalid JSON`); sendJson(res, 400, { error: { message: "Invalid JSON", type: "invalid_request_error" } }); return; }

    const msgCount = (params.messages || []).length;
    const toolCount = (params.tools || []).length;
    const lastMsg = (params.messages || []).slice(-1)[0];
    const rawContent = lastMsg?.content;
    const preview = lastMsg ? `${lastMsg.role}: ${typeof rawContent === "string" ? rawContent.slice(0, 60) : JSON.stringify(rawContent).slice(0, 60)}` : "empty";
    console.log(`[${reqTime}] → POST /v1/chat/completions model=${params.model || DEFAULT_MODEL} stream=${!!params.stream} msgs=${msgCount} tools=${toolCount}`);
    console.log(`[${reqTime}]   last: ${preview}`);

    // Always use server-side token
    const authToken = process.env.NOAH_AUTH_TOKEN || "";
    if (!authToken) { console.log(`[${reqTime}] ← 401 No token`); sendJson(res, 401, { error: { message: "Missing NOAH_AUTH_TOKEN env var", type: "authentication_error" } }); return; }

    const extraCookies = req.headers["x-extra-cookies"] || params.extra_cookies || process.env.NOAH_EXTRA_COOKIES || "";

    try { await handleCompletions(params, authToken, extraCookies, res); console.log(`[${reqTime}] ← 200 OK`); }
    catch (err) {
      console.error(`[${reqTime}] ← 500 Error: ${err.message}`);
      if (!res.headersSent) sendJson(res, 500, { error: { message: err.message, type: "server_error" } });
    }

  } else if (url === "/v1/models" && req.method === "GET") {
    sendJson(res, 200, {
      object: "list",
      data: [
        { id: "gpt-5.3", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gpt-5.5", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gpt-5.4", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gpt-5.5-thinking", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "gemini-3", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "doubao-pro", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "claude", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "us.anthropic.claude-opus-4-7", object: "model", created: 1700000000, owned_by: "noah-ai" },
        { id: "Qwen-27B", object: "model", created: 1700000000, owned_by: "noah-ai" },
      ],
    });

  } else if (url === "/health" || url === "/pool") {
    sendJson(res, 200, { status: "ok", version: "4.0.0", upstream: NOAH_BASE, conversations: convMgr.stats() });

  } else {
    sendJson(res, 404, { error: { message: "Not found", type: "invalid_request_error" } });
  }
});

// ─── Startup ────────────────────────────────────────────────────────────────

async function main() {
  const authToken = process.env.NOAH_AUTH_TOKEN;
  if (!authToken) {
    console.error("[Fatal] NOAH_AUTH_TOKEN environment variable is required");
    process.exit(1);
  }

  server.listen(PORT, HOST, () => {
    console.log(`\nnoah-ai-proxy v4.0 — Incremental Session Affinity`);
    console.log(`Listening: http://${HOST}:${PORT}`);
    console.log(`Upstream:  ${NOAH_BASE}`);
    console.log(`GptsId:    ${DEFAULT_GPTS_ID}`);
    console.log(`Default model: ${DEFAULT_MODEL}`);
    console.log(`Conversations: ${convMgr.conversations.size} cached`);
    console.log(`\nEndpoints:`);
    console.log(`  POST /v1/chat/completions`);
    console.log(`  GET  /v1/models`);
    console.log(`  GET  /health`);
  });
}

main();

process.on("SIGTERM", () => { server.close(); process.exit(0); });
process.on("SIGINT", () => { server.close(); process.exit(0); });
