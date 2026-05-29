/**
 * litellm-bridge v1.1 — Bedrock to OpenAI API format adapter
 * Zero dependencies. Pass ABSK key as Bearer token.
 * Supports: streaming, tool calling, all Claude models, HTTP proxy (https_proxy).
 */
const http = require("http");
const https = require("https");
const net = require("net");
const tls = require("tls");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "4100", 10);
const HOST = process.env.HOST || "127.0.0.1";
const REGION = process.env.BEDROCK_REGION || "us-east-1";
const DEFAULT_MODEL = process.env.DEFAULT_MODEL || "us.anthropic.claude-opus-4-7-v1";
const MAX_TOKENS = 8192;

const MODEL_MAP = {
  "claude-opus-4-7": "us.anthropic.claude-opus-4-7-v1",
  "claude-opus-4-6": "us.anthropic.claude-opus-4-6-v1",
  "claude-opus-4-5": "us.anthropic.claude-opus-4-5-v1",
  "claude-sonnet-4-6": "us.anthropic.claude-sonnet-4-6-v1",
  "claude-sonnet-4-5": "us.anthropic.claude-sonnet-4-5-v1",
  "claude-sonnet-4": "us.anthropic.claude-sonnet-4-20250514",
  "claude-haiku-4-5": "us.anthropic.claude-haiku-4-5-20251001",
  "claude-3.5-sonnet": "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
  "claude-3-haiku": "us.anthropic.claude-3-haiku-20240307-v1:0",
};

function getProxy() {
  const p = process.env.https_proxy || process.env.HTTPS_PROXY || process.env.http_proxy || process.env.HTTP_PROXY || "";
  if (!p) return null;
  try {
    const u = new URL(p.startsWith("http") ? p : "http://" + p);
    return { host: u.hostname, port: parseInt(u.port) || 1080 };
  } catch { return null; }
}

function connectViaProxy(proxy, targetHost, targetPort) {
  return new Promise((resolve, reject) => {
    const conn = net.connect(proxy.port, proxy.host, () => {
      conn.write(`CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\nHost: ${targetHost}:${targetPort}\r\n\r\n`);
    });
    conn.once("error", reject);
    let buf = "";
    const onData = (chunk) => {
      buf += chunk.toString();
      if (buf.includes("\r\n\r\n")) {
        conn.removeListener("data", onData);
        const statusLine = buf.split("\r\n")[0];
        const code = parseInt(statusLine.split(" ")[1]);
        if (code === 200) {
          const tlsSock = tls.connect({ socket: conn, servername: targetHost }, () => resolve(tlsSock));
          tlsSock.once("error", reject);
        } else {
          reject(new Error("Proxy CONNECT failed: " + statusLine));
        }
      }
    };
    conn.on("data", onData);
    setTimeout(() => reject(new Error("Proxy connect timeout")), 10000);
  });
}

function resolveModel(m) {
  if (!m) return DEFAULT_MODEL;
  if (MODEL_MAP[m]) return MODEL_MAP[m];
  if (m.startsWith("us.anthropic.") || m.startsWith("anthropic.")) return m;
  return m;
}

function extractText(c) {
  if (typeof c === "string") return c;
  if (Array.isArray(c)) return c.filter(p => p.type === "text").map(p => p.text).join("\n");
  return String(c || "");
}

function convertContent(c) {
  if (typeof c === "string") return c;
  if (Array.isArray(c)) {
    return c.map(p => {
      if (p.type === "text") return { type: "text", text: p.text };
      if (p.type === "image_url") {
        const url = p.image_url?.url || "";
        if (url.startsWith("data:")) {
          const m = url.match(/^data:(image\/\w+);base64,(.+)$/);
          if (m) return { type: "image", source: { type: "base64", media_type: m[1], data: m[2] } };
        }
        return { type: "text", text: "[Image: " + url + "]" };
      }
      return p;
    });
  }
  return String(c || "");
}

function convertMessages(msgs) {
  let system = "";
  const out = [];
  for (const msg of msgs) {
    if (msg.role === "system") { system += (system ? "\n\n" : "") + extractText(msg.content); continue; }
    if (msg.role === "user") { out.push({ role: "user", content: convertContent(msg.content) }); }
    else if (msg.role === "assistant") {
      const content = [];
      if (msg.content) content.push({ type: "text", text: typeof msg.content === "string" ? msg.content : extractText(msg.content) });
      if (msg.tool_calls) {
        for (const tc of msg.tool_calls) {
          content.push({ type: "tool_use", id: tc.id, name: tc.function.name, input: typeof tc.function.arguments === "string" ? JSON.parse(tc.function.arguments) : tc.function.arguments });
        }
      }
      out.push({ role: "assistant", content });
    } else if (msg.role === "tool") {
      out.push({ role: "user", content: [{ type: "tool_result", tool_use_id: msg.tool_call_id, content: msg.content || "" }] });
    }
  }
  const merged = [];
  for (const msg of out) {
    if (merged.length > 0 && merged[merged.length - 1].role === msg.role) {
      const prev = merged[merged.length - 1];
      const pc = Array.isArray(prev.content) ? prev.content : [{ type: "text", text: prev.content }];
      const cc = Array.isArray(msg.content) ? msg.content : [{ type: "text", text: msg.content }];
      prev.content = [...pc, ...cc];
    } else { merged.push(msg); }
  }
  return { system, messages: merged };
}

function convertTools(tools) {
  if (!tools || !tools.length) return undefined;
  return tools.map(t => { const fn = t.function || t; return { name: fn.name, description: fn.description || "", input_schema: fn.parameters || { type: "object", properties: {} } }; });
}

function callBedrock(modelId, body, apiKey, stream) {
  return new Promise(async (resolve, reject) => {
    const ep = stream ? "invoke-with-response-stream" : "invoke";
    const hostname = "bedrock-runtime." + REGION + ".amazonaws.com";
    const p = "/model/" + modelId + "/" + ep;
    const payload = JSON.stringify(body);
    const headers = { "Content-Type": "application/json", "Authorization": "Bearer " + apiKey, "Content-Length": Buffer.byteLength(payload) };

    const proxy = getProxy();
    if (proxy) {
      try {
        const tlsSock = await connectViaProxy(proxy, hostname, 443);
        const req = https.request({ socket: tlsSock, hostname, port: 443, path: p, method: "POST", headers, createConnection: () => tlsSock }, resolve);
        req.on("error", reject);
        req.write(payload);
        req.end();
      } catch (e) { reject(e); }
    } else {
      const req = https.request({ hostname, port: 443, path: p, method: "POST", headers }, resolve);
      req.on("error", reject);
      req.write(payload);
      req.end();
    }
  });
}

function parseBedrockStream(upstream, onEvent, onEnd, onError) {
  let buf = Buffer.alloc(0);
  upstream.on("data", (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 4) {
      const tl = buf.readUInt32BE(0);
      if (buf.length < tl) break;
      const msg = buf.slice(0, tl); buf = buf.slice(tl);
      try {
        const hl = msg.readUInt32BE(4);
        const payload = msg.slice(12 + hl, tl - 4);
        if (payload.length > 0) {
          const p = JSON.parse(payload.toString("utf-8"));
          if (p.bytes) onEvent(JSON.parse(Buffer.from(p.bytes, "base64").toString("utf-8")));
          else if (p.type) onEvent(p);
        }
      } catch {}
    }
  });
  upstream.on("end", onEnd);
  upstream.on("error", onError);
}

function toOpenAI(br, model, cid) {
  const message = { role: "assistant", content: null };
  let fr = "stop"; const tcs = [];
  if (br.content) {
    const texts = [];
    for (const b of br.content) {
      if (b.type === "text") texts.push(b.text);
      if (b.type === "tool_use") tcs.push({ id: b.id, type: "function", function: { name: b.name, arguments: JSON.stringify(b.input) } });
    }
    if (texts.length) message.content = texts.join("");
    if (tcs.length) { message.tool_calls = tcs; fr = "tool_calls"; }
  }
  if (br.stop_reason === "tool_use") fr = "tool_calls";
  else if (br.stop_reason === "max_tokens") fr = "length";
  return { id: "chatcmpl-" + cid, object: "chat.completion", created: Math.floor(Date.now()/1000), model, choices: [{ index: 0, message, finish_reason: fr }], usage: { prompt_tokens: br.usage?.input_tokens||0, completion_tokens: br.usage?.output_tokens||0, total_tokens: (br.usage?.input_tokens||0)+(br.usage?.output_tokens||0) } };
}

function streamHandler(upstream, res, model, cid) {
  res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive", "Access-Control-Allow-Origin": "*", "X-Accel-Buffering": "no" });
  const w = d => res.write("data: " + JSON.stringify(d) + "\n\n");
  w({ id: "chatcmpl-"+cid, object: "chat.completion.chunk", created: Math.floor(Date.now()/1000), model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });
  let tci = -1;
  parseBedrockStream(upstream, (ev) => {
    if (ev.type === "content_block_start" && ev.content_block?.type === "tool_use") {
      tci++;
      w({ id: "chatcmpl-"+cid, object: "chat.completion.chunk", created: Math.floor(Date.now()/1000), model, choices: [{ index: 0, delta: { tool_calls: [{ index: tci, id: ev.content_block.id, type: "function", function: { name: ev.content_block.name, arguments: "" } }] }, finish_reason: null }] });
    } else if (ev.type === "content_block_delta") {
      if (ev.delta?.type === "text_delta" && ev.delta.text) {
        w({ id: "chatcmpl-"+cid, object: "chat.completion.chunk", created: Math.floor(Date.now()/1000), model, choices: [{ index: 0, delta: { content: ev.delta.text }, finish_reason: null }] });
      } else if (ev.delta?.type === "input_json_delta" && ev.delta.partial_json) {
        w({ id: "chatcmpl-"+cid, object: "chat.completion.chunk", created: Math.floor(Date.now()/1000), model, choices: [{ index: 0, delta: { tool_calls: [{ index: tci, function: { arguments: ev.delta.partial_json } }] }, finish_reason: null }] });
      }
    } else if (ev.type === "message_delta") {
      let fr = "stop";
      if (ev.delta?.stop_reason === "tool_use") fr = "tool_calls";
      else if (ev.delta?.stop_reason === "max_tokens") fr = "length";
      w({ id: "chatcmpl-"+cid, object: "chat.completion.chunk", created: Math.floor(Date.now()/1000), model, choices: [{ index: 0, delta: {}, finish_reason: fr }] });
    }
  }, () => { res.write("data: [DONE]\n\n"); res.end(); }, (e) => { if (!res.writableEnded) { res.write("data: [DONE]\n\n"); res.end(); } });
}

async function handle(params, apiKey, res) {
  const cid = crypto.randomUUID().replace(/-/g,"").slice(0,24);
  const rm = params.model || DEFAULT_MODEL;
  const mid = resolveModel(rm);
  const stream = params.stream || false;
  const { system, messages } = convertMessages(params.messages || []);
  const tools = convertTools(params.tools);
  const body = { anthropic_version: "bedrock-2023-05-31", max_tokens: params.max_tokens || MAX_TOKENS, messages };
  if (system) body.system = system;
  if (tools) body.tools = tools;
  if (params.temperature != null) body.temperature = params.temperature;
  if (params.top_p != null) body.top_p = params.top_p;
  if (params.stop) body.stop_sequences = Array.isArray(params.stop) ? params.stop : [params.stop];
  const proxy = getProxy();
  console.log("[" + cid + "] model=" + mid + " stream=" + stream + " msgs=" + messages.length + " tools=" + (tools?.length||0) + (proxy ? " proxy=" + proxy.host + ":" + proxy.port : " direct"));
  try {
    const up = await callBedrock(mid, body, apiKey, stream);
    if (up.statusCode >= 400) {
      let eb = ""; await new Promise(r => { up.on("data", c => eb+=c); up.on("end", r); });
      console.error("[" + cid + "] Bedrock " + up.statusCode + ": " + eb.slice(0,200));
      json(res, up.statusCode === 403 ? 401 : up.statusCode, { error: { message: "Bedrock: " + eb.slice(0,200), type: "upstream_error" } }); return;
    }
    if (stream) { streamHandler(up, res, rm, cid); }
    else {
      let b = ""; await new Promise((ok, no) => { up.on("data", c => b+=c); up.on("end", ok); up.on("error", no); });
      const br = JSON.parse(b);
      const r = toOpenAI(br, rm, cid);
      console.log("[" + cid + "] done: " + r.usage.total_tokens + " tokens");
      json(res, 200, r);
    }
  } catch(e) { console.error("[" + cid + "] " + e.message); json(res, 500, { error: { message: e.message } }); }
}

function json(res, s, d) { res.writeHead(s, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }); res.end(JSON.stringify(d)); }

http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") { res.writeHead(204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, Authorization" }); res.end(); return; }
  const url = req.url.split("?")[0];
  if (url === "/v1/chat/completions" && req.method === "POST") {
    let body = ""; await new Promise(r => { req.on("data", c => body+=c); req.on("end", r); });
    let params; try { params = JSON.parse(body); } catch { json(res, 400, { error: { message: "Invalid JSON" } }); return; }
    const key = (req.headers.authorization || "").replace(/^Bearer\s+/i, "");
    if (!key) { json(res, 401, { error: { message: "Missing API key" } }); return; }
    await handle(params, key, res);
  } else if (url === "/v1/models") {
    json(res, 200, { object: "list", data: Object.entries(MODEL_MAP).map(([id, bid]) => ({ id, object: "model", created: 1700000000, owned_by: "anthropic" })) });
  } else if (url === "/health") {
    const proxy = getProxy();
    json(res, 200, { status: "ok", version: "1.1.0", region: REGION, default_model: DEFAULT_MODEL, proxy: proxy ? proxy.host + ":" + proxy.port : "none" });
  } else { json(res, 404, { error: { message: "Not found" } }); }
}).listen(PORT, HOST, () => {
  const proxy = getProxy();
  console.log("[litellm-bridge v1.1] http://" + HOST + ":" + PORT);
  console.log("[litellm-bridge] Region: " + REGION + " | Default: " + DEFAULT_MODEL);
  console.log("[litellm-bridge] Proxy: " + (proxy ? proxy.host + ":" + proxy.port : "direct (no proxy)"));
  console.log("[litellm-bridge] Models: " + Object.keys(MODEL_MAP).join(", "));
});
