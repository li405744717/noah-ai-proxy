# noah-ai-proxy

OpenAI-compatible bridge that routes through `ai.noahgroup.com` — the company-approved, InfoSec-compliant API path.

## Architecture

```
codex / openhanako / openclaw (your machine)
        │ OpenAI /v1/chat/completions format
        ▼
noah-ai-proxy (127.0.0.1:4000)
        │ HTTPS to ai.noahgroup.com (approved domain)
        ▼
Noah AI Platform (streamChat API)
        │
        ▼
Claude / GPT (backend model)
```

## Features

- Full OpenAI Chat Completions API compatibility
- Streaming (SSE) and non-streaming responses
- Tool/function calling support
- Multi-turn conversation
- Zero external dependencies (pure Node.js built-ins)
- Runs on localhost only — no network exposure

## Quick Start

```bash
# No npm install needed — zero dependencies!
node index.js
```

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `4000` | Local listen port |
| `HOST` | `127.0.0.1` | Bind address |
| `NOAH_BASE` | `https://ai.noahgroup.com` | Upstream API domain |
| `NOAH_AUTH_TOKEN` | (none) | Default JWT token (can also pass per-request) |
| `NOAH_EXTRA_COOKIES` | (none) | Extra cookies if needed (acw_tc, etc.) |

## Authentication

Pass your `NOAH_AI_AUTH_TOKEN` JWT in one of these ways:

1. **Per-request header** (recommended): `Authorization: Bearer <token>`
2. **Environment variable**: `NOAH_AUTH_TOKEN=<token> node index.js`
3. **Custom header**: `x-auth-token: <token>`

Token source: Login to https://ai.noahgroup.com → DevTools → Application → Cookies → `NOAH_AI_AUTH_TOKEN`

## Usage

### Python (openai SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:4000/v1",
    api_key="<your NOAH_AI_AUTH_TOKEN>"
)

# Streaming
stream = client.chat.completions.create(
    model="gpt-5.5",
    messages=[{"role": "user", "content": "Hello"}],
    stream=True
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="")
```

### Node.js (openai SDK)

```javascript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://127.0.0.1:4000/v1",
  apiKey: "<your NOAH_AI_AUTH_TOKEN>",
});

const stream = await client.chat.completions.create({
  model: "gpt-5.5",
  messages: [{ role: "user", content: "Hello" }],
  stream: true,
});
for await (const chunk of stream) {
  process.stdout.write(chunk.choices[0]?.delta?.content || "");
}
```

### curl

```bash
curl http://127.0.0.1:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"model":"gpt-5.5","messages":[{"role":"user","content":"Hello"}],"stream":false}'
```

### Tool Calling

```python
response = client.chat.completions.create(
    model="gpt-5.5",
    messages=[{"role": "user", "content": "Read the file config.json"}],
    tools=[{
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a file from disk",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }
        }
    }]
)
# Returns: choices[0].message.tool_calls = [{id, function: {name, arguments}}]
```

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/chat/completions` | POST | Chat completions (OpenAI format) |
| `/v1/models` | GET | Available models |
| `/health` | GET | Health check |

## Available Models

- `gpt-5.5` — Default, recommended
- `gpt-5.5-thinking` — With reasoning/thinking
- `gpt-4o`
- `gpt-4o-mini`
- `deepseek-r1`

## OpenAI Compatibility Checklist

| Feature | Status |
|---------|--------|
| `POST /v1/chat/completions` | ✅ |
| `GET /v1/models` | ✅ |
| Streaming (SSE) | ✅ |
| Non-streaming | ✅ |
| Multi-turn messages | ✅ |
| System messages | ✅ |
| Tool/function calling | ✅ |
| `finish_reason: stop` | ✅ |
| `finish_reason: tool_calls` | ✅ |
| Proper `id` field format | ✅ |
| Proper `created` timestamp | ✅ |
| `usage` object | ✅ (zeros — upstream doesn't report) |
| CORS headers | ✅ |
| Error format `{error:{message,type}}` | ✅ |

## Security

- Binds to `127.0.0.1` only — invisible to network
- All traffic goes to the approved `ai.noahgroup.com` domain
- No local credential files — token passed at runtime
- No extra ports, no tunnels, no proxy chains
