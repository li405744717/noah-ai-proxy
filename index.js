const http = require("http");
const https = require("https");
const { v4: uuidv4 } = require("uuid");
const express = require("express");
const config = require("./config");

const app = express();
app.use(express.json({ limit: "10mb" }));

function verifyAuth(req, res, next) {
  if (!config.auth.apiKey) return next();
  const authHeader = req.headers.authorization || "";
  const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  const xApiKey = req.headers["x-api-key"] || "";
  if (bearer === config.auth.apiKey || xApiKey === config.auth.apiKey) {
    return next();
  }
  return res.status(401).json({ error: { message: "Invalid API key", type: "authentication_error" } });
}

app.use(verifyAuth);

app.get("/v1/models", (req, res) => {
  res.json({
    object: "list",
    data: [
      { id: "claude-opus-4-6", object: "model", created: Date.now(), owned_by: "noah-ai-proxy" },
      { id: "claude-sonnet-4-6", object: "model", created: Date.now(), owned_by: "noah-ai-proxy" },
      { id: "claude-agent", object: "model", created: Date.now(), owned_by: "noah-ai-proxy" }
    ]
  });
});

app.post("/v1/chat/completions", async (req, res) => {
  const { messages, stream, model } = req.body;
  const prompt = messagesToPrompt(messages);

  try {
    const runData = await createRun(prompt);
    const runId = runData.runId;

    if (stream) {
      await handleStream(res, runId, model);
    } else {
      await handleNonStream(res, runId, model);
    }
  } catch (err) {
    if (!res.headersSent) {
      res.status(500).json({ error: { message: err.message, type: "server_error" } });
    }
  }
});

function messagesToPrompt(messages) {
  if (!messages || messages.length === 0) return "";
  if (messages.length === 1) return extractContent(messages[0]);
  return messages.map((m) => {
    const role = m.role === "assistant" ? "Assistant" : m.role === "system" ? "System" : "Human";
    return `${role}: ${extractContent(m)}`;
  }).join("\n\n");
}

function extractContent(msg) {
  if (typeof msg.content === "string") return msg.content;
  if (Array.isArray(msg.content)) {
    return msg.content.filter((p) => p.type === "text").map((p) => p.text).join("\n");
  }
  return String(msg.content || "");
}

function createRun(prompt) {
  return new Promise((resolve, reject) => {
    const boundary = `----FormBoundary${uuidv4().replace(/-/g, "")}`;
    const fields = {
      prompt,
      sessionId: "new",
      idempotencyKey: `idem_${uuidv4().replace(/-/g, "")}`,
      session_mode: "one_shot"
    };

    let body = "";
    for (const [key, value] of Object.entries(fields)) {
      body += `--${boundary}\r\n`;
      body += `Content-Disposition: form-data; name="${key}"\r\n\r\n`;
      body += `${value}\r\n`;
    }
    body += `--${boundary}--\r\n`;

    const url = new URL(`${config.gateway.baseUrl}/v1/runs`);
    const isHttps = url.protocol === "https:";
    const transport = isHttps ? https : http;

    const options = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname,
      method: "POST",
      headers: {
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "Content-Length": Buffer.byteLength(body),
        "GW-USER-ID": config.gateway.userId,
        "GW-TENANT-ID": config.gateway.tenantId,
        ...config.gateway.extraHeaders
      },
      rejectUnauthorized: false
    };

    const req = transport.request(options, (response) => {
      let data = "";
      response.on("data", (chunk) => (data += chunk));
      response.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.code === 200 && parsed.data) {
            resolve(parsed.data);
          } else {
            reject(new Error(`Gateway createRun failed: ${parsed.message || data}`));
          }
        } catch (e) {
          reject(new Error(`Gateway response parse error: ${data.slice(0, 200)}`));
        }
      });
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

function streamEvents(runId) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${config.gateway.baseUrl}/v1/runs/events`);
    const isHttps = url.protocol === "https:";
    const transport = isHttps ? https : http;

    const options = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: `${url.pathname}?runId=${encodeURIComponent(runId)}`,
      method: "POST",
      headers: {
        "GW-USER-ID": config.gateway.userId,
        "GW-TENANT-ID": config.gateway.tenantId,
        "Accept": "text/event-stream",
        ...config.gateway.extraHeaders
      },
      rejectUnauthorized: false
    };

    const req = transport.request(options, (response) => {
      if (response.statusCode !== 200) {
        let body = "";
        response.on("data", (c) => (body += c));
        response.on("end", () => reject(new Error(`Events endpoint returned ${response.statusCode}: ${body}`)));
        return;
      }
      resolve(response);
    });
    req.on("error", reject);
    req.end();
  });
}

async function handleStream(res, runId, model) {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");

  const chatId = `chatcmpl-${uuidv4().replace(/-/g, "")}`;
  const created = Math.floor(Date.now() / 1000);

  const sseStream = await streamEvents(runId);
  let buffer = "";

  sseStream.on("data", (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (line.startsWith("data:")) {
        const jsonStr = line.slice(5).trim();
        if (!jsonStr) continue;
        try {
          const event = JSON.parse(jsonStr);
          const text = extractTextDelta(event);
          if (text) {
            const chunk = {
              id: chatId,
              object: "chat.completion.chunk",
              created,
              model: model || "claude-agent",
              choices: [{ index: 0, delta: { content: text }, finish_reason: null }]
            };
            res.write(`data: ${JSON.stringify(chunk)}\n\n`);
          }

          if (isRunComplete(event)) {
            const doneChunk = {
              id: chatId,
              object: "chat.completion.chunk",
              created,
              model: model || "claude-agent",
              choices: [{ index: 0, delta: {}, finish_reason: "stop" }]
            };
            res.write(`data: ${JSON.stringify(doneChunk)}\n\n`);
            res.write("data: [DONE]\n\n");
            res.end();
            return;
          }
        } catch {}
      }
    }
  });

  sseStream.on("end", () => {
    if (!res.writableEnded) {
      const doneChunk = {
        id: chatId,
        object: "chat.completion.chunk",
        created,
        model: model || "claude-agent",
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }]
      };
      res.write(`data: ${JSON.stringify(doneChunk)}\n\n`);
      res.write("data: [DONE]\n\n");
      res.end();
    }
  });

  sseStream.on("error", (err) => {
    if (!res.writableEnded) {
      res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`);
      res.end();
    }
  });
}

async function handleNonStream(res, runId, model) {
  const sseStream = await streamEvents(runId);
  let buffer = "";
  let resultText = "";

  sseStream.on("data", (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (line.startsWith("data:")) {
        const jsonStr = line.slice(5).trim();
        if (!jsonStr) continue;
        try {
          const event = JSON.parse(jsonStr);
          if (event.type === "assistant" && event.subtype === "text_delta") {
            const content = event.message?.content;
            if (Array.isArray(content)) {
              for (const block of content) {
                if (block.type === "text") resultText += block.text;
              }
            }
          } else if (event.type === "stream_event") {
            const delta = event.event?.delta;
            if (delta?.type === "text_delta" && delta.text) {
              resultText += delta.text;
            }
          } else if (event.type === "result" && event.result) {
            resultText = event.result;
          }
        } catch {}
      }
    }
  });

  return new Promise((resolve) => {
    sseStream.on("end", () => {
      const chatId = `chatcmpl-${uuidv4().replace(/-/g, "")}`;
      res.json({
        id: chatId,
        object: "chat.completion",
        created: Math.floor(Date.now() / 1000),
        model: model || "claude-agent",
        choices: [{
          index: 0,
          message: { role: "assistant", content: resultText || "(no response)" },
          finish_reason: "stop"
        }],
        usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
      });
      resolve();
    });

    sseStream.on("error", (err) => {
      if (!res.headersSent) {
        res.status(500).json({ error: { message: err.message, type: "server_error" } });
      }
      resolve();
    });
  });
}

function extractTextDelta(event) {
  if (event.type === "assistant" && event.subtype === "text_delta") {
    const content = event.message?.content;
    if (Array.isArray(content)) {
      return content.filter((b) => b.type === "text").map((b) => b.text).join("");
    }
  }
  if (event.type === "stream_event") {
    const delta = event.event?.delta;
    if (delta?.type === "text_delta" && delta.text) {
      return delta.text;
    }
  }
  return "";
}

function isRunComplete(event) {
  if (event.type === "result") return true;
  if (event.status === "completed" || event.status === "failed" || event.status === "canceled") return true;
  if (event.type === "stream_event" && event.event?.type === "message_stop") return true;
  return false;
}

app.get("/health", (req, res) => {
  res.json({ status: "ok", version: "2.0.0", gateway: config.gateway.baseUrl });
});

const server = app.listen(config.port, config.host, () => {
  console.log(`noah-ai-proxy v2 listening on ${config.host}:${config.port}`);
  console.log(`Gateway: ${config.gateway.baseUrl}`);
  console.log(`User: ${config.gateway.userId}`);
  console.log(`Auth: ${config.auth.apiKey ? "enabled" : "disabled"}`);
});

process.on("SIGTERM", () => { server.close(); process.exit(0); });
process.on("SIGINT", () => { server.close(); process.exit(0); });
