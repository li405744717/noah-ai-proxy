# Noah AI → OpenAI 兼容代理

## 项目用途

本项目是一个 Node.js 代理服务，将诺亚AI (ai.noahgroup.com) 的私有对话接口转换为 OpenAI 标准 `/v1/chat/completions` 格式，使任何兼容 OpenAI SDK 的 Agent 框架可以直接调用诺亚AI。

## 快速启动

```bash
node noah_openai_proxy.js
# 默认端口 9876，可通过 PORT 环境变量修改
```

## 调用方式

### 认证

将诺亚AI的 JWT Token (`NOAH_AI_AUTH_TOKEN`) 作为 API Key 传入：

```
Authorization: Bearer <你的NOAH_AI_AUTH_TOKEN>
```

Token 获取方式：登录 https://ai.noahgroup.com → 浏览器开发者工具 → Application → Cookies → 复制 `NOAH_AI_AUTH_TOKEN` 的值。

### 请求格式（完全兼容 OpenAI）

```json
POST http://localhost:9876/v1/chat/completions
{
  "model": "gpt-5.5-thinking",
  "messages": [
    {"role": "system", "content": "你是一个助手"},
    {"role": "user", "content": "你好"}
  ],
  "stream": true
}
```

### 可选参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `model` | 模型名称 | `gpt-5.5-thinking` |
| `stream` | 是否流式输出 | `false` |
| `gpts_id` | GPTs应用ID | `76`（纯净版） |
| `extra_cookies` | 额外cookie（某些网络环境需要） | 空 |

### 支持的模型

- `gpt-5.5-thinking` — 推荐，带推理能力
- `gpt-4o`
- `gpt-4o-mini`
- `deepseek-r1`

### 额外Cookie传递

如果仅靠 Token 无法访问（被WAF拦截等），可通过以下方式传递额外cookie：

- Header: `x-extra-cookies: acw_tc=xxx; UA_AUTH_SID=yyy`
- Body: `"extra_cookies": "acw_tc=xxx; UA_AUTH_SID=yyy"`

## 内部原理

```
Agent/SDK 请求 → [本代理] 
  → 1. POST /api/noah-chat-svc/session/createSession (获取sessionId)
  → 2. POST /api/noah-chat-svc/chat/streamChat (发送消息，收SSE流)
  → 转换 Noah SSE (event:data + {"chunk":"xxx"}) 为 OpenAI SSE 格式
→ 返回给 Agent/SDK
```

## SDK 调用示例

### Python

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:9876/v1",
    api_key="eyJhbGciOiJIUzI1NiJ9...",  # NOAH_AI_AUTH_TOKEN
)

response = client.chat.completions.create(
    model="gpt-5.5-thinking",
    messages=[{"role": "user", "content": "分析当前A股市场"}],
    stream=True,
)
for chunk in response:
    print(chunk.choices[0].delta.content or "", end="")
```

### Node.js

```javascript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:9876/v1",
  apiKey: "eyJhbGciOiJIUzI1NiJ9...",
});

const stream = await client.chat.completions.create({
  model: "gpt-5.5-thinking",
  messages: [{ role: "user", content: "你好" }],
  stream: true,
});
for await (const chunk of stream) {
  process.stdout.write(chunk.choices[0]?.delta?.content || "");
}
```

### curl

```bash
curl http://localhost:9876/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"model":"gpt-5.5-thinking","messages":[{"role":"user","content":"你好"}],"stream":false}'
```

## 端点列表

| 端点 | 方法 | 说明 |
|------|------|------|
| `/v1/chat/completions` | POST | 对话补全（兼容OpenAI） |
| `/v1/models` | GET | 可用模型列表 |
| `/health` | GET | 健康检查 |

## 依赖

零外部依赖，仅使用 Node.js 内置模块（http, https, crypto）。要求 Node.js >= 16。

## 文件结构

- `noah_openai_proxy.js` — 主程序，代理服务实现
- `package.json` — 项目元数据
- `README.md` — 项目说明
- `CLAUDE.md` — 本文件，AI Agent 上下文指令
