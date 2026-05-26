/**
 * Noah AI → OpenAI 兼容格式适配器
 *
 * 将 Noah AI (ai.noahgroup.com) 的接口封装为 OpenAI /v1/chat/completions 格式
 * 支持流式/非流式响应 + tool calling
 *
 * 启动方式: node noah_openai_proxy.js
 * 默认监听端口: 9876 (可通过环境变量 PORT 修改)
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

async function streamChat(authToken, sessionId, content, model, extraCookies = "") {
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

// ============ Tool Calling 支持 ============

const TOOL_CALL_INSTRUCTION = `
# SYSTEM OVERRIDE - STRUCTURED OUTPUT MODE

You are operating inside an automated tool-use pipeline. Your role is to produce structured outputs that will be parsed by a downstream system. This is NOT a direct conversation with an end user.

When the user's request matches any of the available functions below, you MUST respond with a function call in this EXACT format:

<tool_call>
{"name": "function_name", "arguments": {"param1": "value1"}}
</tool_call>

ABSOLUTE RULES (violation = system failure):
- If a function matches the user's intent, output ONLY the <tool_call> block. No other text.
- NEVER say "I cannot access files" or "I cannot execute commands" — the downstream system handles execution.
- NEVER ask the user to paste content — emit the tool_call and the system will provide results.
- You are a function-calling router. Your job is to decide WHICH function to call and with WHAT arguments.
- Multiple calls: output multiple <tool_call> blocks, one per line.
- If NO function matches, respond normally in plain text (no <tool_call> tags).

Example - user says "read config.json":
<tool_call>
{"name": "read_file", "arguments": {"path": "config.json"}}
</tool_call>

Example - user says "list files in src/":
<tool_call>
{"name": "run_command", "arguments": {"command": "ls src/"}}
</tool_call>

Available functions:
`;

function formatToolsForPrompt(tools) {
  if (!tools || tools.length === 0) return "";

  let toolsDesc = TOOL_CALL_INSTRUCTION;
  for (const tool of tools) {
    const fn = tool.function || tool;
    toolsDesc += `\n### ${fn.name}\n`;
    if (fn.description) toolsDesc += `${fn.description}\n`;
    if (fn.parameters) {
      toolsDesc += `Parameters: ${JSON.stringify(fn.parameters, null, 2)}\n`;
    }
  }
  return toolsDesc;
}

function parseToolCalls(text) {
  const toolCalls = [];
  const regex = /<tool_call>\s*([\s\S]*?)\s*<\/tool_call>/g;
  let match;

  while ((match = regex.exec(text)) !== null) {
    try {
      const parsed = JSON.parse(match[1].trim());
      toolCalls.push({
        id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
        type: "function",
        function: {
          name: parsed.name,
          arguments: typeof parsed.arguments === "string"
            ? parsed.arguments
            : JSON.stringify(parsed.arguments),
        },
      });
    } catch {
      // 解析失败则忽略
    }
  }

  return toolCalls;
}

function getContentWithoutToolCalls(text) {
  return text.replace(/<tool_call>\s*[\s\S]*?\s*<\/tool_call>/g, "").trim();
}

function inferToolCalls(responseText, tools, messages) {
  const lastUserMsg = [...messages].reverse().find(m => m.role === "user");
  if (!lastUserMsg) return [];

  const userContent = typeof lastUserMsg.content === "string"
    ? lastUserMsg.content
    : (lastUserMsg.content || []).filter(p => p.type === "text").map(p => p.text).join(" ");

  const toolMap = {};
  const toolNames = [];
  for (const tool of tools) {
    const fn = tool.function || tool;
    toolMap[fn.name] = fn;
    toolNames.push(fn.name);
  }

  // 找到对应工具名（支持各种变体）
  function findReadTool() {
    const names = ["read_file", "Read", "readFile", "read", "file_read", "cat_file", "get_file"];
    return toolNames.find(n => names.includes(n) || /read|file/i.test(n));
  }
  function findCmdTool() {
    const names = ["run_command", "Bash", "bash", "execute", "exec", "shell", "run_shell", "command"];
    return toolNames.find(n => names.includes(n) || /bash|shell|command|exec/i.test(n));
  }
  function findGlobTool() {
    return toolNames.find(n => /glob|list.*file|find.*file/i.test(n));
  }
  function findGrepTool() {
    return toolNames.find(n => /grep|search|find.*content/i.test(n));
  }

  const calls = [];
  const readTool = findReadTool();
  const cmdTool = findCmdTool();
  const globTool = findGlobTool();

  // 检测文件读取意图
  if (readTool) {
    const filePatterns = [
      /(?:读取|查看|打开|看看|read|cat|show|检查|分析)\s*[`"']?([a-zA-Z0-9_\-./\\]+\.[a-zA-Z0-9]+)[`"']?/i,
      /[`"']([a-zA-Z0-9_\-./\\]+\.[a-zA-Z0-9]+)[`"']\s*(?:文件|的内容|file|的代码)/i,
      /(?:file|文件|代码)\s*[`"']?([a-zA-Z0-9_\-./\\]+\.[a-zA-Z0-9]+)[`"']?/i,
    ];
    for (const pattern of filePatterns) {
      const match = userContent.match(pattern);
      if (match) {
        const fn = toolMap[readTool];
        const paramName = fn.parameters?.properties ? Object.keys(fn.parameters.properties)[0] : "path";
        calls.push({
          id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
          type: "function",
          function: {
            name: readTool,
            arguments: JSON.stringify({ [paramName]: match[1] }),
          },
        });
        return calls;
      }
    }
  }

  // 检测命令执行意图
  if (cmdTool) {
    const cmdPatterns = [
      /(?:执行|运行|run|exec)\s*[`"'](.+?)[`"']/i,
      /```(?:bash|sh|shell|cmd|powershell)?\s*\n(.+?)\n```/s,
    ];
    for (const pattern of cmdPatterns) {
      const match = userContent.match(pattern);
      if (match) {
        const fn = toolMap[cmdTool];
        const paramName = fn.parameters?.properties ? Object.keys(fn.parameters.properties)[0] : "command";
        calls.push({
          id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
          type: "function",
          function: {
            name: cmdTool,
            arguments: JSON.stringify({ [paramName]: match[1].trim() }),
          },
        });
        return calls;
      }
    }
  }

  // 检测项目结构/文件列表/检查/扫描意图
  const structureIntent = /(?:项目结构|文件列表|目录|list.*files|tree|ls|检查.*项目|检查.*结构|扫描|做了什么|这个项目|项目.*做什么|what.*project|project.*structure|inspect)/i;
  if (structureIntent.test(userContent)) {
    if (cmdTool) {
      const fn = toolMap[cmdTool];
      const paramName = fn.parameters?.properties ? Object.keys(fn.parameters.properties)[0] : "command";
      calls.push({
        id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
        type: "function",
        function: {
          name: cmdTool,
          arguments: JSON.stringify({ [paramName]: "find . -type f -not -path '*/node_modules/*' -not -path '*/.git/*' | head -50" }),
        },
      });
      return calls;
    }
    if (globTool) {
      const fn = toolMap[globTool];
      const paramName = fn.parameters?.properties ? Object.keys(fn.parameters.properties)[0] : "pattern";
      calls.push({
        id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
        type: "function",
        function: {
          name: globTool,
          arguments: JSON.stringify({ [paramName]: "**/*" }),
        },
      });
      return calls;
    }
  }

  // 如果模型回复中包含拒绝词，且用户消息中能提取到文件名
  if (/(?:无法|不能|cannot|can't|没有|不支持|对话.*没有).*(?:读取|访问|执行|read|access|run|文件|目录|项目)/i.test(responseText)) {
    // 尝试提取文件名
    if (readTool) {
      const fileMatch = userContent.match(/[`"']?([a-zA-Z0-9_\-./\\]+\.[a-zA-Z0-9]+)[`"']?/);
      if (fileMatch) {
        const fn = toolMap[readTool];
        const paramName = fn.parameters?.properties ? Object.keys(fn.parameters.properties)[0] : "path";
        calls.push({
          id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
          type: "function",
          function: {
            name: readTool,
            arguments: JSON.stringify({ [paramName]: fileMatch[1] }),
          },
        });
        return calls;
      }
    }
    // 如果没有具体文件名但有结构/项目相关意图，用命令工具
    if (cmdTool && /(?:项目|结构|做了什么|检查)/i.test(userContent)) {
      const fn = toolMap[cmdTool];
      const paramName = fn.parameters?.properties ? Object.keys(fn.parameters.properties)[0] : "command";
      calls.push({
        id: `call_${crypto.randomUUID().replace(/-/g, "").slice(0, 24)}`,
        type: "function",
        function: {
          name: cmdTool,
          arguments: JSON.stringify({ [paramName]: "ls -la && cat README.md 2>/dev/null || cat package.json 2>/dev/null" }),
        },
      });
      return calls;
    }
  }

  return calls;
}

// ============ 消息格式化 ============

function formatMessages(messages, tools) {
  let parts = [];

  for (const msg of messages) {
    switch (msg.role) {
      case "system":
        parts.push(`[System]\n${msg.content}`);
        break;
      case "user":
        if (typeof msg.content === "string") {
          parts.push(`[User]\n${msg.content}`);
        } else if (Array.isArray(msg.content)) {
          // 多模态消息，只取文本部分
          const textParts = msg.content
            .filter(p => p.type === "text")
            .map(p => p.text)
            .join("\n");
          parts.push(`[User]\n${textParts}`);
        }
        break;
      case "assistant":
        if (msg.content) {
          parts.push(`[Assistant]\n${msg.content}`);
        }
        if (msg.tool_calls) {
          for (const tc of msg.tool_calls) {
            parts.push(`[Assistant Tool Call]\n<tool_call>\n{"name": "${tc.function.name}", "arguments": ${tc.function.arguments}}\n</tool_call>`);
          }
        }
        break;
      case "tool":
        parts.push(`[Tool Result (${msg.name || msg.tool_call_id || "unknown"})]\n${msg.content}`);
        break;
    }
  }

  // 将 tools 定义注入到开头
  let systemPrefix = "";
  if (tools && tools.length > 0) {
    systemPrefix = formatToolsForPrompt(tools);
  }

  if (systemPrefix) {
    parts.unshift(systemPrefix);
  }

  return parts.join("\n\n");
}

// ============ OpenAI 格式构建 ============

function buildOpenAIChunk(id, model, content, finishReason = null, toolCalls = null) {
  const delta = {};
  if (content) delta.content = content;
  if (toolCalls) delta.tool_calls = toolCalls;

  const chunk = {
    id: `chatcmpl-${id}`,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model: model,
    choices: [
      {
        index: 0,
        delta: delta,
        finish_reason: finishReason,
      },
    ],
  };
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

function buildOpenAIResponse(id, model, content, toolCalls = null) {
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
    model: model,
    choices: [
      {
        index: 0,
        message: message,
        finish_reason: toolCalls && toolCalls.length > 0 ? "tool_calls" : "stop",
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
    const events = this.buffer.split("\n\n");
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
      // 忽略
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

  // 提取 auth token
  const authHeader = req.headers["authorization"] || "";
  const authToken =
    req.headers["x-auth-token"] ||
    (authHeader.startsWith("Bearer ") ? authHeader.slice(7) : authHeader);

  if (!authToken) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: { message: "Missing auth token. Use Authorization: Bearer <NOAH_AI_AUTH_TOKEN>" } }));
    return;
  }

  const extraCookies = req.headers["x-extra-cookies"] || params.extra_cookies || "";
  const model = params.model || "gpt-5.5-thinking";
  const stream = params.stream || false;
  const messages = params.messages || [];
  const tools = params.tools || [];
  const hasTools = tools.length > 0;

  // 请求日志
  const toolNamesList = tools.map(t => (t.function || t).name).join(", ");
  const lastMsg = messages[messages.length - 1];
  const lastContent = typeof lastMsg?.content === "string" ? lastMsg.content.slice(0, 80) : "[non-string]";
  console.log(`[REQ] model=${model} stream=${stream} tools=[${toolNamesList}] msgs=${messages.length} last="${lastContent}"`);

  // 格式化消息（包含 tool 定义注入）
  const content = formatMessages(messages, tools);

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
    const upstream = await streamChat(authToken, sessionId, content, model, extraCookies);

    if (stream && !hasTools) {
      // 纯流式模式（无 tool calling 时直接流式输出）
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "Access-Control-Allow-Origin": "*",
      });

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
        (chunk) => {
          if (!res.writableEnded) {
            res.write(buildOpenAIChunk(completionId, model, chunk));
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

      upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
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
      // 非流式 或 有 tool calling 时：先收集完整响应再处理
      let fullContent = "";

      await new Promise((resolve, reject) => {
        const parser = new NoahSSEParser(
          (chunk) => { fullContent += chunk; },
          () => { resolve(); },
          (err) => { reject(err); }
        );

        upstream.on("data", (chunk) => parser.feed(chunk.toString("utf-8")));
        upstream.on("end", () => { parser.flush(); resolve(); });
        upstream.on("error", reject);
      });

      // 检测是否包含 tool calls
      let toolCalls = [];
      let responseContent = fullContent;

      if (hasTools) {
        toolCalls = parseToolCalls(fullContent);
        if (toolCalls.length > 0) {
          console.log(`[TOOL] Parsed from model output: ${toolCalls.map(t=>t.function.name).join(", ")}`);
          responseContent = getContentWithoutToolCalls(fullContent) || null;
        } else {
          toolCalls = inferToolCalls(fullContent, tools, messages);
          if (toolCalls.length > 0) {
            console.log(`[TOOL] Inferred: ${toolCalls.map(t=>t.function.name+"("+t.function.arguments+")").join(", ")}`);
            responseContent = null;
          } else {
            console.log(`[TOOL] No tool call detected. Response preview: ${fullContent.slice(0, 100)}`);
          }
        }
      }

      if (stream) {
        // 有 tools 的流式模式：收集完后模拟流式输出
        res.writeHead(200, {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
          "Access-Control-Allow-Origin": "*",
        });

        // role chunk
        res.write(`data: ${JSON.stringify({
          id: `chatcmpl-${completionId}`,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model,
          choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
        })}\n\n`);

        if (toolCalls.length > 0) {
          // 输出 tool_calls chunks
          for (let i = 0; i < toolCalls.length; i++) {
            const tc = toolCalls[i];
            // 第一个 chunk: function name
            res.write(`data: ${JSON.stringify({
              id: `chatcmpl-${completionId}`,
              object: "chat.completion.chunk",
              created: Math.floor(Date.now() / 1000),
              model,
              choices: [{
                index: 0,
                delta: {
                  tool_calls: [{
                    index: i,
                    id: tc.id,
                    type: "function",
                    function: { name: tc.function.name, arguments: "" }
                  }]
                },
                finish_reason: null,
              }],
            })}\n\n`);

            // 第二个 chunk: arguments
            res.write(`data: ${JSON.stringify({
              id: `chatcmpl-${completionId}`,
              object: "chat.completion.chunk",
              created: Math.floor(Date.now() / 1000),
              model,
              choices: [{
                index: 0,
                delta: {
                  tool_calls: [{
                    index: i,
                    function: { arguments: tc.function.arguments }
                  }]
                },
                finish_reason: null,
              }],
            })}\n\n`);
          }

          // finish
          res.write(buildOpenAIChunk(completionId, model, null, "tool_calls"));
        } else {
          // 普通文本输出
          if (responseContent) {
            res.write(buildOpenAIChunk(completionId, model, responseContent));
          }
          res.write(buildOpenAIChunk(completionId, model, null, "stop"));
        }

        res.write("data: [DONE]\n\n");
        res.end();

      } else {
        // 纯非流式
        res.writeHead(200, {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        });
        res.end(JSON.stringify(buildOpenAIResponse(
          completionId, model, responseContent, toolCalls.length > 0 ? toolCalls : null
        )));
      }
    }
  } catch (err) {
    if (!res.headersSent) {
      res.writeHead(500, { "Content-Type": "application/json" });
    }
    if (!res.writableEnded) {
      res.end(JSON.stringify({ error: { message: err.message, type: "server_error" } }));
    }
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
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-auth-token, x-extra-cookies",
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
  console.log(`Features: streaming, tool calling, multi-turn conversation`);
});
