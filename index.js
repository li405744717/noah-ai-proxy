/**
 * noah-ai-proxy v4.3 — OpenAI-compatible bridge via ai.noahgroup.com
 *
 * v4.3 improvements over v4.2:
 * - Smart prompt deduplication: system prompt + tools only sent once per session
 * - Conversation identity based on system prompt hash (stable across turns)
 * - sentCount properly tracks what Noah already has in context
 * - Tool schema fingerprint: only re-inject if tools actually change
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
const POOL_SIZE = parseInt(process.env.POOL_SIZE || "10", 10);
const MAX_CHUNK_BYTES = 12000;

// ─── Utility ────────────────────────────────────────────────────────────────

function uuid() { return crypto.randomUUID(); }
function shortId() { return crypto.randomUUID().replace(/-/g, "").slice(0, 24); }
function parseJwt(token) { try { return JSON.parse(Buffer.from(token.split(".")[1], "base64").toString()); } catch { return {}; } }
function md5(str) { return crypto.createHash("md5").update(str).digest("hex"); }

function httpsReq(url, opts, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const reqOpts = { hostname: u.hostname, port: 443, path: u.pathname + u.search, method: opts.method || "POST", headers: opts.headers || {} };
    const req = https.request(reqOpts, (res) => {
      if (opts.stream) { resolve(res); return; }
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => { try { resolve({ status: res.statusCode, body: JSON.parse(data) }); } catch { resolve({ status: res.statusCode, body: data }); } });
    });
    req.on("error", reject);
    if (body) req.write(typeof body === "string" ? body : JSON.stringify(body));
    req.end();
  });
}

// ─── Session Pool ───────────────────────────────────────────────────────────

const POOL_FILE = path.join(__dirname, ".sessions.json");

class SessionPool {
  constructor(size, prefix = "proxy-pool") {
    this.size = size;
    this.prefix = prefix;
    this.sessions = [];
    this.waitQueue = [];
    this.initialized = false;
  }

  async init(authToken, extraCookies, gptsId) {
    const existing = await this._fetchExisting(authToken, extraCookies, gptsId);
    console.log(`[Pool] Found ${existing.length} existing sessions`);
    for (const id of existing.slice(0, this.size)) {
      this.sessions.push({ id, busy: false, useCount: 0, convId: null, sentCount: 0, systemHash: null, toolsHash: null, toolsInjected: false });
    }
    const needed = this.size - this.sessions.length;
    if (needed > 0) {
      console.log(`[Pool] Creating ${needed} new sessions...`);
      const results = await Promise.allSettled(Array.from({ length: needed }, (_, i) => this._create(authToken, extraCookies, gptsId, this.sessions.length + i)));
      for (const r of results) {
        if (r.status === "fulfilled") this.sessions.push({ id: r.value, busy: false, useCount: 0, convId: null, sentCount: 0, systemHash: null, toolsHash: null, toolsInjected: false });
        else console.error(`[Pool] Create failed: ${r.reason?.message}`);
      }
    }
    this._save();
    this.initialized = true;
    console.log(`[Pool] Ready: ${this.sessions.length}/${this.size}`);
    if (this.sessions.length === 0) throw new Error("No sessions");
  }

  acquire(convId) {
    // 1. Same conversation (reuse context)
    const bound = this.sessions.find(s => !s.busy && s.convId === convId);
    if (bound) { bound.busy = true; bound.useCount++; return Promise.resolve(bound); }
    // 2. Free unbound
    const free = this.sessions.find(s => !s.busy && s.convId === null);
    if (free) { free.convId = convId; free.busy = true; free.useCount++; return Promise.resolve(free); }
    // 3. LRU steal
    const lru = this.sessions.filter(s => !s.busy).sort((a, b) => a.useCount - b.useCount)[0];
    if (lru) { lru.convId = convId; lru.sentCount = 0; lru.systemHash = null; lru.toolsHash = null; lru.toolsInjected = false; lru.busy = true; lru.useCount++; return Promise.resolve(lru); }
    // 4. Wait
    return new Promise(resolve => { this.waitQueue.push({ resolve, convId }); });
  }

  release(session) {
    session.busy = false;
    if (this.waitQueue.length > 0) {
      const { resolve, convId } = this.waitQueue.shift();
      if (session.convId !== convId) { session.convId = convId; session.sentCount = 0; session.systemHash = null; session.toolsHash = null; session.toolsInjected = false; }
      session.busy = true; session.useCount++;
      resolve(session);
    }
  }

  stats() {
    const busy = this.sessions.filter(s => s.busy).length;
    return { total: this.sessions.length, busy, free: this.sessions.length - busy, waiting: this.waitQueue.length };
  }

  async _fetchExisting(authToken, extraCookies, gptsId) {
    try {
      const res = await httpsReq(`${NOAH_BASE}/api/noah-chat-svc/session/listSessionRecord?pageNo=1&pageSize=50`, { method: "GET", headers: buildHeaders(authToken, extraCookies) }, null);
      if (res.status === 200 && res.body?.code === 200 && Array.isArray(res.body.data)) return res.body.data.filter(r => r.name?.startsWith(`${this.prefix}-`) && r.gptsId === gptsId).map(r => r.id).filter(Boolean);
    } catch {}
    return this._loadDisk();
  }

  _loadDisk() { try { if (fs.existsSync(POOL_FILE)) return JSON.parse(fs.readFileSync(POOL_FILE, "utf-8")).sessionIds || []; } catch {} return []; }
  _save() { try { fs.writeFileSync(POOL_FILE, JSON.stringify({ sessionIds: this.sessions.map(s => s.id), updatedAt: new Date().toISOString() }, null, 2)); } catch {} }

  async _create(authToken, extraCookies, gptsId, index) {
    const res = await httpsReq(`${NOAH_BASE}/api/noah-chat-svc/session/createSession`, { method: "POST", headers: buildHeaders(authToken, extraCookies) }, { gptsId, sessionName: `${this.prefix}-${index}` });
    if (res.status === 200 && res.body?.code === 200 && res.body.data) return res.body.data.sessionId || res.body.data.id || res.body.data;
    throw new Error(`createSession: ${JSON.stringify(res.body).slice(0, 200)}`);
  }
}

const pool = new SessionPool(POOL_SIZE, "proxy-pool");

// ─── Noah AI API ────────────────────────────────────────────────────────────

function buildHeaders(authToken, extraCookies) {
  return { "Content-Type": "application/json", Cookie: `NOAH_AI_AUTH_TOKEN=${authToken}${extraCookies ? "; " + extraCookies : ""}`, Origin: NOAH_BASE, Referer: `${NOAH_BASE}/home`, "User-Agent": "Mozilla/5.0 (compatible; noah-ai-proxy/4.3)", "request-id": uuid() };
}

function streamChatBody(authToken, sessionId, content, model) {
  const tokenInfo = parseJwt(authToken);
  const workNo = tokenInfo.workNo || tokenInfo.userUid || "proxy";
  return { sessionId, chatType: 0, content, currentGptsId: "", assistantType: 0, model: model || DEFAULT_MODEL, pluginIds: [2, 3], fileIds: [], chatRequestId: `${workNo}${Date.now()}${Math.floor(Math.random() * 1000)}` };
}

async function callStreamChat(authToken, sessionId, content, model, extraCookies) {
  return httpsReq(`${NOAH_BASE}/api/noah-chat-svc/chat/streamChat`, { method: "POST", headers: buildHeaders(authToken, extraCookies), stream: true }, streamChatBody(authToken, sessionId, content, model));
}

async function sendAndWait(authToken, sessionId, content, model, extraCookies) {
  const upstream = await callStreamChat(authToken, sessionId, content, model, extraCookies);
  if (upstream.statusCode >= 400) { const body = await collectRawBody(upstream); throw new Error(`upstream ${upstream.statusCode}: ${body.slice(0, 100)}`); }
  return collectResponse(upstream);
}

// ─── Noah SSE Parser ────────────────────────────────────────────────────────

class NoahSSEParser {
  constructor({ onText, onDone, onError }) { this.buffer = ""; this.onText = onText; this.onDone = onDone; this.onError = onError; this.done = false; }
  feed(raw) { if (this.done) return; this.buffer += raw; const blocks = this.buffer.split("\n\n"); this.buffer = blocks.pop() || ""; for (const block of blocks) this._parse(block.trim()); }
  flush() { if (!this.done && this.buffer.trim()) { this._parse(this.buffer.trim()); this.buffer = ""; } }
  _parse(block) {
    if (!block || this.done) return;
    let eventType = "", dataLine = "";
    for (const line of block.split("\n")) { if (line.startsWith("event:")) eventType = line.slice(6).trim(); else if (line.startsWith("data:")) dataLine = line.slice(5).trim(); }
    if (eventType === "end" || eventType === "stop" || eventType === "done" || dataLine === "[DONE]") { this.done = true; this.onDone(); return; }
    if (eventType === "error") { this.done = true; this.onError(new Error(dataLine || "upstream error")); return; }
    if (!dataLine) return;
    try { const d = JSON.parse(dataLine); const text = d.chunk || d.content || d.data?.content || ""; if (text) this.onText(text); } catch {}
  }
}

// ─── OpenAI Format Builders ─────────────────────────────────────────────────

function openaiChunk(id, model, content, finishReason) {
  return { id: `chatcmpl-${id}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: content !== null ? { content } : {}, finish_reason: finishReason || null }] };
}
function openaiToolCallChunks(id, model, toolCalls) {
  const chunks = [];
  for (let i = 0; i < toolCalls.length; i++) {
    const tc = toolCalls[i];
    chunks.push({ id: `chatcmpl-${id}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { tool_calls: [{ index: i, id: tc.id, type: "function", function: { name: tc.function.name, arguments: "" } }] }, finish_reason: null }] });
    chunks.push({ id: `chatcmpl-${id}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { tool_calls: [{ index: i, function: { arguments: tc.function.arguments } }] }, finish_reason: null }] });
  }
  return chunks;
}
function openaiResponse(id, model, content, toolCalls) {
  const message = { role: "assistant" };
  if (toolCalls && toolCalls.length > 0) { message.content = content || null; message.tool_calls = toolCalls; }
  else { message.content = content; }
  return { id: `chatcmpl-${id}`, object: "chat.completion", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, message, finish_reason: toolCalls?.length > 0 ? "tool_calls" : "stop" }], usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 } };
}

// ─── Tool Calling Support ───────────────────────────────────────────────────

function buildToolSchemaChunks(tools) {
  const header = `[SYSTEM CONFIGURATION - Function Definitions]
You are in STRICT function-calling mode. Do NOT execute actions yourself.
When the user gives a task, respond ONLY with <tool_call> blocks:
<tool_call>
{"name": "function_name", "arguments": {"param1": "value1"}}
</tool_call>
Rules: ONLY <tool_call> blocks. No text. No explanations. Multiple calls = multiple blocks.
If no function matches, reply in plain text.

Here are the available functions (part PART_NUM of TOTAL_PARTS):
`;
  const chunks = [];
  let currentTools = [];
  let currentSize = 0;
  const headerSize = Buffer.byteLength(header, 'utf-8');

  for (const tool of tools) {
    const fn = tool.function || tool;
    const paramNames = fn.parameters?.properties ? Object.keys(fn.parameters.properties).join(", ") : "";
    const required = fn.parameters?.required ? fn.parameters.required.join(", ") : "";
    let toolStr = `### ${fn.name}\n`;
    if (fn.description) toolStr += `${fn.description.slice(0, 100)}\n`;
    toolStr += `Params: ${paramNames}\n`;
    if (required) toolStr += `Required: ${required}\n`;
    toolStr += "\n";
    const toolSize = Buffer.byteLength(toolStr, 'utf-8');
    if (currentSize + toolSize + headerSize > MAX_CHUNK_BYTES && currentTools.length > 0) { chunks.push(currentTools.join("")); currentTools = []; currentSize = 0; }
    currentTools.push(toolStr);
    currentSize += toolSize;
  }
  if (currentTools.length > 0) chunks.push(currentTools.join(""));
  return chunks.map((body, i) => header.replace("PART_NUM", String(i + 1)).replace("TOTAL_PARTS", String(chunks.length)) + body);
}

function getToolsHash(tools) {
  if (!tools || tools.length === 0) return null;
  return md5(tools.map(t => (t.function || t).name).sort().join(","));
}

function parseToolCallsFromText(text) {
  const calls = [];
  const re = /<tool_call>\s*([\s\S]*?)\s*<\/tool_call>/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    try { const parsed = JSON.parse(m[1].trim()); calls.push({ id: `call_${shortId()}`, type: "function", function: { name: parsed.name, arguments: typeof parsed.arguments === "string" ? parsed.arguments : JSON.stringify(parsed.arguments) } }); } catch {}
  }
  return calls;
}

function stripToolCalls(text) { return text.replace(/<tool_call>\s*[\s\S]*?\s*<\/tool_call>/g, "").trim(); }

// ─── Message Formatting (v4.3: Smart Deduplication) ─────────────────────────

function extractText(msg) {
  if (typeof msg.content === "string") return msg.content;
  if (Array.isArray(msg.content)) return msg.content.filter(p => p.type === "text").map(p => p.text).join("\n");
  return String(msg.content || "");
}

/**
 * v4.3 key change: Conversation identity is based on system prompt hash.
 * This is stable across all turns of the same agent conversation.
 */
function getConversationId(messages) {
  const sys = messages.find(m => m.role === "system");
  return md5(sys ? extractText(sys) : "no-system");
}

/**
 * v4.3: Only send messages that Noah doesn't already have.
 * 
 * Agent pattern: every request sends [system, user1, asst1, tool1, user2, asst2, tool2, ..., userN]
 * Noah session already has all messages up to sentCount.
 * We only need to send messages[sentCount:] — but skip system (already sent in first turn).
 */
function formatNewMessages(messages, sentCount, hasTools) {
  // First turn: include system prompt
  if (sentCount === 0) {
    const parts = [];
    const sys = messages.find(m => m.role === "system");
    if (sys) parts.push(`[System Instructions]\n${extractText(sys)}`);
    
    // Include all non-system messages
    for (const msg of messages) {
      if (msg.role === "system") continue;
      parts.push(formatSingleMessage(msg));
    }
    
    if (hasTools) parts.push(getToolReminder(messages));
    return joinAndTruncate(parts);
  }
  
  // Subsequent turns: only new messages (skip system + already-sent)
  const newMessages = messages.slice(sentCount);
  if (newMessages.length === 0) return null;
  
  const parts = [];
  for (const msg of newMessages) {
    if (msg.role === "system") continue; // Never re-send system prompt
    parts.push(formatSingleMessage(msg));
  }
  
  if (hasTools) parts.push(getToolReminder(messages));
  return joinAndTruncate(parts);
}

function formatSingleMessage(msg) {
  if (msg.role === "user") return `[User]\n${extractText(msg)}`;
  if (msg.role === "assistant") {
    const parts = [];
    if (msg.content) parts.push(`[Assistant]\n${typeof msg.content === 'string' ? msg.content : extractText(msg)}`);
    if (msg.tool_calls) {
      for (const tc of msg.tool_calls) {
        parts.push(`[Assistant Tool Call]\n<tool_call>\n{"name":"${tc.function.name}","arguments":${tc.function.arguments}}\n</tool_call>`);
      }
    }
    return parts.join("\n\n");
  }
  if (msg.role === "tool") {
    const c = msg.content || "";
    const truncated = c.length > 800 ? c.slice(0, 800) + "...[truncated]" : c;
    return `[Tool Result (${msg.name || msg.tool_call_id || "?"})]\n${truncated}`;
  }
  return "";
}

function getToolReminder(messages) {
  const lastMsg = messages[messages.length - 1];
  if (lastMsg && lastMsg.role === "tool") {
    return `[Reminder] Tool execution complete. If more actions needed, output <tool_call> blocks. If task is done, provide a brief text summary.`;
  }
  return `[Reminder] Respond ONLY with <tool_call> blocks when an action is needed. No explanations.`;
}

function joinAndTruncate(parts) {
  const content = parts.filter(Boolean).join("\n\n");
  if (Buffer.byteLength(content, 'utf-8') > MAX_CHUNK_BYTES) {
    // Keep system (if present) + last 3 messages + reminder
    console.log(`[Format] Content too large (${Buffer.byteLength(content, 'utf-8')}B), truncating...`);
    const keep = parts.slice(0, 1).concat(parts.slice(-4)); // first (system) + last 4 (including reminder)
    return keep.filter(Boolean).join("\n\n");
  }
  return content;
}

// ─── Request Handler ────────────────────────────────────────────────────────

async function handleCompletions(reqBody, authToken, extraCookies, res) {
  const model = reqBody.model || DEFAULT_MODEL;
  const stream = reqBody.stream || false;
  const messages = reqBody.messages || [];
  const tools = reqBody.tools || [];
  const hasTools = tools.length > 0;

  const completionId = shortId();
  const convId = getConversationId(messages);
  const toolsHash = getToolsHash(tools);

  const session = await pool.acquire(convId);
  
  // Check if tools changed (agent might update tool definitions mid-conversation)
  const needToolReinject = hasTools && (!session.toolsInjected || session.toolsHash !== toolsHash);
  
  console.log(`[${completionId}] conv=${convId.slice(0,8)} sess=${session.id} sent=${session.sentCount} tools=${tools.length} needInject=${needToolReinject}`);

  try {
    // Step 1: Inject tool schema if needed
    if (needToolReinject) {
      const chunks = buildToolSchemaChunks(tools);
      console.log(`[${completionId}] Injecting ${chunks.length} tool schema chunk(s)...`);
      for (let i = 0; i < chunks.length; i++) {
        console.log(`[${completionId}]   chunk ${i+1}/${chunks.length}: ${Buffer.byteLength(chunks[i], 'utf-8')}B`);
        await sendAndWait(authToken, session.id, chunks[i], model, extraCookies);
      }
      session.toolsInjected = true;
      session.toolsHash = toolsHash;
      console.log(`[${completionId}] Tool schema injection complete`);
    }

    // Step 2: Format and send only NEW messages
    const content = formatNewMessages(messages, session.sentCount, hasTools);
    if (!content) {
      pool.release(session);
      sendJson(res, 400, { error: { message: "No new messages to send", type: "invalid_request_error" } });
      return;
    }

    const contentBytes = Buffer.byteLength(content, 'utf-8');
    console.log(`[${completionId}] Sending ${contentBytes}B (msgs ${session.sentCount}->${messages.length})`);

    const upstream = await callStreamChat(authToken, session.id, content, model, extraCookies);
    
    if (upstream.statusCode >= 400) {
      const errBody = await collectRawBody(upstream);
      console.log(`[${completionId}] upstream error ${upstream.statusCode}: ${errBody.slice(0, 200)}`);
      // If 405 (WAF), try resetting session
      if (upstream.statusCode === 405) {
        console.log(`[${completionId}] WAF block detected, resetting session state`);
        session.sentCount = 0; session.toolsInjected = false; session.toolsHash = null;
      }
      pool.release(session);
      sendJson(res, 502, { error: { message: `Upstream ${upstream.statusCode}`, type: "upstream_error" } });
      return;
    }

    if (stream && !hasTools) {
      // Pure streaming (no tool parsing)
      res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no" });
      writeSse(res, { id: `chatcmpl-${completionId}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });
      const parser = new NoahSSEParser({
        onText: (text) => { if (!res.writableEnded) writeSse(res, openaiChunk(completionId, model, text, null)); },
        onDone: () => { session.sentCount = messages.length; pool.release(session); finishStream(res, completionId, model); },
        onError: (err) => { pool.release(session); if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.write("data: [DONE]\n\n"); res.end(); } },
      });
      upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
      upstream.on("end", () => { parser.flush(); if (!parser.done) { session.sentCount = messages.length; pool.release(session); finishStream(res, completionId, model); } });
      upstream.on("error", (err) => { pool.release(session); if (!res.writableEnded) { writeSse(res, { error: { message: err.message } }); res.end(); } });
    } else {
      // Collect response (needed for tool call parsing)
      const fullText = await collectResponse(upstream);
      session.sentCount = messages.length;
      pool.release(session);

      console.log(`[${completionId}] response: ${fullText.length} chars`);

      let toolCalls = [];
      let responseContent = fullText;
      if (hasTools) {
        toolCalls = parseToolCallsFromText(fullText);
        console.log(`[${completionId}] parsed ${toolCalls.length} tool_call(s)`);
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
    console.error(`[${completionId}] error: ${err.message}`);
    if (!res.headersSent) sendJson(res, 500, { error: { message: err.message, type: "server_error" } });
    else if (!res.writableEnded) { res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`); res.end(); }
  }
}

function collectResponse(upstream) {
  return new Promise((resolve, reject) => {
    let full = "", resolved = false, rawChunks = [];
    const done = () => { if (resolved) return; resolved = true; if (full.length === 0 && rawChunks.length > 0) { const raw = rawChunks.join(''); try { const e = JSON.parse(raw); if (e.code && e.code !== 200) { reject(new Error(`upstream: ${e.msg || raw.slice(0, 100)}`)); return; } } catch {} } resolve(full); };
    const parser = new NoahSSEParser({ onText: (t) => { full += t; }, onDone: () => { done(); }, onError: (e) => { if (!resolved) { resolved = true; reject(e); } } });
    upstream.on("data", (c) => { const s = c.toString("utf-8"); rawChunks.push(s); parser.feed(s); });
    upstream.on("end", () => { parser.flush(); done(); });
    upstream.on("error", (e) => { if (!resolved) { resolved = true; reject(e); } });
  });
}

function collectRawBody(stream) { return new Promise(resolve => { let d = ""; stream.on("data", c => (d += c.toString("utf-8"))); stream.on("end", () => resolve(d)); stream.on("error", () => resolve(d)); }); }
function finishStream(res, id, model) { if (res.writableEnded) return; writeSse(res, openaiChunk(id, model, null, "stop")); res.write("data: [DONE]\n\n"); res.end(); }
function writeSse(res, data) { res.write(`data: ${JSON.stringify(data)}\n\n`); }
function sendJson(res, status, data) { res.writeHead(status, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }); res.end(JSON.stringify(data)); }

// ─── HTTP Server ────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") { res.writeHead(204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, Authorization, x-auth-token, x-extra-cookies" }); res.end(); return; }
  const url = req.url.split("?")[0];

  if (url === "/v1/chat/completions" && req.method === "POST") {
    let body = "";
    await new Promise(r => { req.on("data", c => (body += c)); req.on("end", r); });
    let params;
    try { params = JSON.parse(body); } catch { sendJson(res, 400, { error: { message: "Invalid JSON" } }); return; }
    const authToken = process.env.NOAH_AUTH_TOKEN || "";
    if (!authToken) { sendJson(res, 401, { error: { message: "Missing NOAH_AUTH_TOKEN" } }); return; }
    if (!pool.initialized) { sendJson(res, 503, { error: { message: "Pool not ready" } }); return; }
    const extraCookies = req.headers["x-extra-cookies"] || params.extra_cookies || process.env.NOAH_EXTRA_COOKIES || "";
    try { await handleCompletions(params, authToken, extraCookies, res); } catch (err) { console.error(`[Error] ${err.message}`); if (!res.headersSent) sendJson(res, 500, { error: { message: err.message } }); }

  } else if (url === "/v1/models" && req.method === "GET") {
    sendJson(res, 200, { object: "list", data: [
      { id: "gpt-5.3", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "gpt-5.5", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "gpt-5.5-thinking", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "gemini-3", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "claude", object: "model", created: 1700000000, owned_by: "noah-ai" },
      { id: "us.anthropic.claude-opus-4-7", object: "model", created: 1700000000, owned_by: "noah-ai" },
    ]});

  } else if (url === "/health" || url === "/pool") {
    sendJson(res, 200, { status: "ok", version: "4.3.0", upstream: NOAH_BASE, pool: pool.stats() });
  } else {
    sendJson(res, 404, { error: { message: "Not found" } });
  }
});

// ─── Startup ────────────────────────────────────────────────────────────────

async function main() {
  const authToken = process.env.NOAH_AUTH_TOKEN;
  if (!authToken) { console.error("[Fatal] NOAH_AUTH_TOKEN required. Set via environment variable."); process.exit(1); }
  const extraCookies = process.env.NOAH_EXTRA_COOKIES || "";
  try { await pool.init(authToken, extraCookies, DEFAULT_GPTS_ID); } catch (err) { console.error(`[Fatal] Pool init: ${err.message}`); process.exit(1); }
  server.listen(PORT, HOST, () => {
    console.log(`\nnoah-ai-proxy v4.3 — Smart Deduplication`);
    console.log(`Listening: http://${HOST}:${PORT}`);
    console.log(`Upstream:  ${NOAH_BASE}`);
    console.log(`Pool:      ${pool.sessions.length} sessions (gptsId=${DEFAULT_GPTS_ID})`);
    console.log(`Max chunk: ${MAX_CHUNK_BYTES}B`);
    console.log(`Default:   ${DEFAULT_MODEL}\n`);
  });
}

main();
process.on("SIGTERM", () => { server.close(); process.exit(0); });
process.on("SIGINT", () => { server.close(); process.exit(0); });
