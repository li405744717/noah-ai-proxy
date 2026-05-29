const express = require("express");
const http = require("http");
const { v4: uuidv4 } = require("uuid");
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
      { id: "claude-agent", object: "model", created: Date.now(), owned_by: "noah-ai-proxy" },
      { id: "claude-sonnet-4-6", object: "model", created: Date.now(), owned_by: "noah-ai-proxy" },
      { id: "claude-opus-4-6", object: "model", created: Date.now(), owned_by: "noah-ai-proxy" }
    ]
  });
});

app.post("/v1/chat/completions", async (req, res) => {
  const { messages, stream, model } = req.body;

  const prompt = messagesToPrompt(messages);
  const runId = `run_${uuidv4().replace(/-/g, "")}`;

  const sidecarPayload = {
    run_id: runId,
    prompt,
    workspace_path: config.defaults.workspacePath,
    claude_home_path: config.defaults.claudeHomePath,
    permission_mode: config.defaults.permissionMode,
    session_mode: config.defaults.sessionMode,
    timeout_ms: config.defaults.timeoutMs,
    allowed_tools: [],
    settings_sources: [],
    skills_whitelist: [],
    skills_source_dirs: [],
    use_shared_nas: true
  };

  if (stream) {
    await handleStream(req, res, sidecarPayload, model);
  } else {
    await handleNonStream(req, res, sidecarPayload, model);
  }
});

function messagesToPrompt(messages) {
  if (!messages || messages.length === 0) return "";
  const last = messages[messages.length - 1];
  if (messages.length === 1) {
    return extractContent(last);
  }
  const parts = messages.map((m) => {
    const role = m.role === "assistant" ? "Assistant" : m.role === "system" ? "System" : "Human";
    return `${role}: ${extractContent(m)}`;
  });
  return parts.join("\n\n");
}

function extractContent(msg) {
  if (typeof msg.content === "string") return msg.content;
  if (Array.isArray(msg.content)) {
    return msg.content
      .filter((p) => p.type === "text")
      .map((p) => p.text)
      .join("\n");
  }
  return String(msg.content || "");
}

async function handleStream(req, res, payload, model) {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");

  const chatId = `chatcmpl-${uuidv4().replace(/-/g, "")}`;
  const created = Math.floor(Date.now() / 1000);

  try {
    const sseStream = await callSidecarStream(payload);
    let buffer = "";

    sseStream.on("data", (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (line.startsWith("data: [DONE]")) {
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

        if (line.startsWith("data: ")) {
          const jsonStr = line.slice(6);
          try {
            const event = JSON.parse(jsonStr);
            const text = extractTextFromSseEvent(event);
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
  } catch (err) {
    if (!res.writableEnded) {
      res.write(`data: ${JSON.stringify({ error: { message: err.message } })}\n\n`);
      res.end();
    }
  }
}

async function handleNonStream(req, res, payload, model) {
  try {
    const result = await callSidecarNonStream(payload);
    const chatId = `chatcmpl-${uuidv4().replace(/-/g, "")}`;
    res.json({
      id: chatId,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model: model || "claude-agent",
      choices: [{
        index: 0,
        message: { role: "assistant", content: result },
        finish_reason: "stop"
      }],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
    });
  } catch (err) {
    res.status(500).json({ error: { message: err.message, type: "server_error" } });
  }
}

function callSidecarStream(payload) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${config.sidecar.baseUrl}/runs/stream`);
    const postData = JSON.stringify(payload);

    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(postData)
      }
    };

    if (config.sidecar.apiKey) {
      options.headers["x-api-key"] = config.sidecar.apiKey;
    }

    const req = http.request(options, (response) => {
      if (response.statusCode !== 200) {
        let body = "";
        response.on("data", (c) => (body += c));
        response.on("end", () => reject(new Error(`Sidecar returned ${response.statusCode}: ${body}`)));
        return;
      }
      resolve(response);
    });

    req.on("error", reject);
    req.write(postData);
    req.end();
  });
}

function callSidecarNonStream(payload) {
  return new Promise((resolve, reject) => {
    const stream = callSidecarStream(payload);
    stream
      .then((response) => {
        let buffer = "";
        let resultText = "";

        response.on("data", (chunk) => {
          buffer += chunk.toString();
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.startsWith("data: ") && !line.startsWith("data: [DONE]")) {
              try {
                const event = JSON.parse(line.slice(6));
                if (event.type === "result" && event.result) {
                  resultText = event.result;
                } else {
                  const text = extractTextFromSseEvent(event);
                  if (text) resultText += text;
                }
              } catch {}
            }
          }
        });

        response.on("end", () => resolve(resultText || "(no response)"));
        response.on("error", reject);
      })
      .catch(reject);
  });
}

function extractTextFromSseEvent(event) {
  if (event.type === "assistant" && event.subtype === "text_delta") {
    const content = event.message?.content;
    if (Array.isArray(content)) {
      return content
        .filter((block) => block.type === "text")
        .map((block) => block.text)
        .join("");
    }
    if (typeof event.message === "string") return event.message;
  }
  if (event.type === "result" && event.result) {
    return null;
  }
  return "";
}

app.get("/health", (req, res) => {
  res.json({ status: "ok", version: "1.0.0", upstream: config.sidecar.baseUrl });
});

const server = app.listen(config.port, config.host, () => {
  console.log(`noah-ai-proxy listening on ${config.host}:${config.port}`);
  console.log(`Upstream sidecar: ${config.sidecar.baseUrl}`);
  console.log(`Auth: ${config.auth.apiKey ? "enabled" : "disabled"}`);
});

process.on("SIGTERM", () => { server.close(); process.exit(0); });
process.on("SIGINT", () => { server.close(); process.exit(0); });
