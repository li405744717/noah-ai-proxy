module.exports = {
  port: parseInt(process.env.PROXY_PORT || "4000", 10),
  host: process.env.PROXY_HOST || "127.0.0.1",

  gateway: {
    baseUrl: process.env.GATEWAY_URL || "http://127.0.0.1:8090",
    userId: process.env.GW_USER_ID || "proxy-user",
    tenantId: process.env.GW_TENANT_ID || "default",
    extraHeaders: parseHeaders(process.env.GW_EXTRA_HEADERS || "")
  },

  auth: {
    apiKey: process.env.PROXY_API_KEY || ""
  }
};

function parseHeaders(str) {
  if (!str) return {};
  const headers = {};
  for (const pair of str.split(";")) {
    const [key, ...rest] = pair.split(":");
    if (key && rest.length) {
      headers[key.trim()] = rest.join(":").trim();
    }
  }
  return headers;
}
