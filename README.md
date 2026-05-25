# Noah AI → OpenAI 兼容代理

将诺亚 AI (ai.noahgroup.com) 的对话接口封装为 OpenAI `/v1/chat/completions` 标准格式，供任何兼容 OpenAI SDK 的 Agent 框架直接调用。

## 特性

- 完全兼容 OpenAI Chat Completions API
- 支持流式 (SSE) 和非流式响应
- 自动管理会话创建
- 零外部依赖，纯 Node.js 实现

## 快速开始

```bash
# 启动代理服务
node noah_openai_proxy.js

# 或指定端口
PORT=8080 node noah_openai_proxy.js
```

默认监听 `http://localhost:9876`

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/v1/chat/completions` | POST | 对话补全 (兼容 OpenAI) |
| `/v1/models` | GET | 可用模型列表 |
| `/health` | GET | 健康检查 |

## 使用示例

### Python (openai SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:9876/v1",
    api_key="YOUR_NOAH_AI_AUTH_TOKEN",  # JWT token
)

# 流式调用
stream = client.chat.completions.create(
    model="gpt-5.5-thinking",
    messages=[{"role": "user", "content": "分析一下当前市场行情"}],
    stream=True,
)
for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```

### Node.js (openai SDK)

```javascript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:9876/v1",
  apiKey: "YOUR_NOAH_AI_AUTH_TOKEN",
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
  -H "Authorization: Bearer YOUR_NOAH_AI_AUTH_TOKEN" \
  -d '{
    "model": "gpt-5.5-thinking",
    "messages": [{"role": "user", "content": "你好"}],
    "stream": false
  }'
```

## 认证

使用 Noah AI 平台的 JWT Token (`NOAH_AI_AUTH_TOKEN`)，通过以下方式传递：

- `Authorization: Bearer <token>` (推荐)
- `x-auth-token: <token>`

## 支持的模型

- `gpt-5.5-thinking`
- `gpt-4o`
- `gpt-4o-mini`
- `deepseek-r1`

## 额外参数

在请求 body 中可传递：
- `gpts_id`: 指定 GPTs 应用 ID (默认 76，即"纯净版")

## 原理

```
OpenAI SDK → [本代理] → Noah AI createSession + streamChat → [本代理] → OpenAI SSE 格式
```

代理自动完成：
1. 调用 `createSession` 获取 `sessionId`
2. 调用 `streamChat` 发送消息
3. 将 Noah SSE (`event:data` + `{"chunk":"..."}`) 转换为 OpenAI SSE 格式

## License

MIT
