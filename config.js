const path = require("path");

module.exports = {
  port: parseInt(process.env.PROXY_PORT || "4000", 10),
  host: process.env.PROXY_HOST || "127.0.0.1",

  sidecar: {
    baseUrl: process.env.SIDECAR_URL || "http://127.0.0.1:4319",
    apiKey: process.env.SIDECAR_API_KEY || ""
  },

  defaults: {
    workspacePath: process.env.WORKSPACE_PATH || "/var/lib/agent-gw/sessions/proxy-workspace",
    claudeHomePath: process.env.CLAUDE_HOME_PATH || "/var/lib/agent-gw/sessions/proxy-workspace/.claude",
    permissionMode: process.env.PERMISSION_MODE || "acceptEdits",
    sessionMode: process.env.SESSION_MODE || "one_shot",
    timeoutMs: parseInt(process.env.TIMEOUT_MS || "120000", 10)
  },

  auth: {
    apiKey: process.env.PROXY_API_KEY || ""
  }
};
