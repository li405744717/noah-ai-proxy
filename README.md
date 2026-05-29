# noah-ai-proxy

OpenAI-compatible bridge that converts requests to Agent Gateway format. Runs on your local machine, calls the authorized Gateway endpoint — no direct API exposure, no extra credentials.

## Architecture

```
codex / openhanako / openclaw (your machine)
        │ OpenAI format
        ▼
noah-ai-proxy (127.0.0.1:4000)     ← this bridge
        │ Gateway private format (HTTPS)
        ▼
Java Agent Gateway (8090 via approved domain)
        │
        ▼
Claude (Bedrock) — no extra token cost
```

## Quick Start

```bash
npm install

# Minimal — connects to gateway at localhost:8090
node index.js

# Production — connects to remote gateway
GATEWAY_URL=https://your-gateway-domain.com \
GW_USER_ID=your-user-id \
PROXY_API_KEY=sk-your-secret \
node index.js
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PROXY_PORT` | `4000` | Local port for OpenAI-compatible API |
| `PROXY_HOST` | `127.0.0.1` | Bind address (keep localhost for security) |
| `GATEWAY_URL` | `http://127.0.0.1:8090` | Agent Gateway base URL |
| `GW_USER_ID` | `proxy-user` | Gateway user ID header |
| `GW_TENANT_ID` | `default` | Gateway tenant ID header |
| `GW_EXTRA_HEADERS` | (empty) | Extra headers, format: `Key1:Val1;Key2:Val2` |
| `PROXY_API_KEY` | (empty) | Optional API key for this proxy |

## Usage with Agent Frameworks

### openhanako / Codex / any OpenAI-compatible client

```
base_url: http://127.0.0.1:4000/v1
model: claude-agent
api_key: (your PROXY_API_KEY or any string if auth disabled)
```

### Python (openai SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:4000/v1",
    api_key="sk-anything"
)

response = client.chat.completions.create(
    model="claude-agent",
    messages=[{"role": "user", "content": "Hello"}],
    stream=True
)
for chunk in response:
    print(chunk.choices[0].delta.content or "", end="")
```

### curl

```bash
curl http://127.0.0.1:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-agent",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'
```

## Endpoints

- `GET /health` — Health check
- `GET /v1/models` — List available models
- `POST /v1/chat/completions` — Chat completions (stream & non-stream)

## How It Works

1. Receives OpenAI-format request on localhost
2. Converts messages to a prompt string
3. Calls Gateway `POST /v1/runs` (multipart/form-data) to create a run
4. Subscribes to `POST /v1/runs/events?runId=xxx` for SSE stream
5. Converts Gateway SSE events back to OpenAI streaming format
6. Returns to client

## Security

- Bridge runs locally (127.0.0.1 only) — invisible to network scans
- All outbound traffic goes through the approved Gateway domain
- No AWS credentials or API keys stored locally
- Optional `PROXY_API_KEY` for local access control
