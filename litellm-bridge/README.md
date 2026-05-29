# litellm-bridge

Lightweight Bedrock → OpenAI compatible API bridge for agent frameworks.

## Why

Agent frameworks (openhanako, openclaw, etc.) speak OpenAI API format. AWS Bedrock uses a different format with different auth. This bridge translates between them — zero dependencies, single file, ~200 lines.

## Quick Start

```bash
node index.js
# Listening on http://127.0.0.1:4100
```

## Usage

```bash
curl http://127.0.0.1:4100/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ABSK_KEY" \
  -d '{
    "model": "claude-sonnet-4-6",
    "messages": [{"role": "user", "content": "hi"}],
    "stream": true
  }'
```

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `PORT` | 4100 | Listen port |
| `HOST` | 127.0.0.1 | Bind address |
| `BEDROCK_REGION` | us-east-1 | AWS region |
| `DEFAULT_MODEL` | us.anthropic.claude-sonnet-4-6-v1 | Fallback model |

## Supported Models

| Friendly Name | Bedrock Model ID |
|---------------|------------------|
| claude-opus-4-6 | us.anthropic.claude-opus-4-6-v1 |
| claude-opus-4-5 | us.anthropic.claude-opus-4-5-v1 |
| claude-sonnet-4-6 | us.anthropic.claude-sonnet-4-6-v1 |
| claude-sonnet-4-5 | us.anthropic.claude-sonnet-4-5-v1 |
| claude-sonnet-4 | us.anthropic.claude-sonnet-4-20250514 |
| claude-haiku-4-5 | us.anthropic.claude-haiku-4-5-20251001 |
| claude-3.5-sonnet | us.anthropic.claude-3-5-sonnet-20241022-v2:0 |
| claude-3-haiku | us.anthropic.claude-3-haiku-20240307-v1:0 |

You can also pass full Bedrock model IDs directly.

## Features

- **Streaming**: Full SSE streaming support
- **Tool Calling**: OpenAI tool_calls ↔ Bedrock tool_use conversion
- **Multi-turn**: Handles message history with role merging
- **Images**: base64 image_url support
- **Zero deps**: Only Node.js built-ins

## For openhanako/openclaw

```json
{
  "baseUrl": "http://127.0.0.1:4100/v1",
  "apiKey": "YOUR_ABSK_KEY",
  "models": [{"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6"}]
}
```
