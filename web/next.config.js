/** @type {import('next').NextConfig} */

const fs = require("fs");
const path = require("path");

function readJsonFile(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return {};
  }
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return "";
}

function normalizeBoolean(value) {
  if (value === "__NEXT_PUBLIC_AUTH_ENABLED_PLACEHOLDER__") {
    return value;
  }
  return ["1", "true", "yes", "on"].includes(String(value).trim().toLowerCase())
    ? "true"
    : "false";
}

const SETTINGS_DIR = path.resolve(__dirname, "..", "data", "user", "settings");
const SYSTEM_SETTINGS = readJsonFile(path.join(SETTINGS_DIR, "system.json"));
const AUTH_SETTINGS = readJsonFile(path.join(SETTINGS_DIR, "auth.json"));
const BACKEND_PORT = firstNonEmpty(
  process.env.BACKEND_PORT,
  SYSTEM_SETTINGS.backend_port,
  "8001",
);

// Use data/user/settings as the frontend source of truth. Environment values
// remain explicit deployment overrides for Docker/CI.
const NEXT_PUBLIC_API_BASE = firstNonEmpty(
  process.env.NEXT_PUBLIC_API_BASE_EXTERNAL,
  SYSTEM_SETTINGS.next_public_api_base_external,
  process.env.NEXT_PUBLIC_API_BASE,
  SYSTEM_SETTINGS.next_public_api_base,
  `http://localhost:${BACKEND_PORT}`,
);

const NEXT_PUBLIC_AUTH_ENABLED = normalizeBoolean(
  firstNonEmpty(
    process.env.NEXT_PUBLIC_AUTH_ENABLED,
    process.env.AUTH_ENABLED,
    AUTH_SETTINGS.enabled,
    "false",
  ),
);

process.env.NEXT_PUBLIC_API_BASE = NEXT_PUBLIC_API_BASE;
process.env.NEXT_PUBLIC_AUTH_ENABLED = NEXT_PUBLIC_AUTH_ENABLED;

// Resolve the build-time application version from the single source of
// truth at ``deeptutor/__version__.py``. The Python file is parsed with a
// small regex so the JS build does not need to execute Python.
const APP_VERSION = (() => {
  try {
    const text = fs.readFileSync(
      path.resolve(__dirname, "..", "deeptutor", "__version__.py"),
      "utf8",
    );
    const match = text.match(/__version__\s*=\s*["']([^"']+)["']/);
    if (match) return match[1];
  } catch {}
  return "";
})();

// Platform API service URL (used by rewrites proxy).
// Defaults to localhost:8100 for local dev. In Docker Compose, set
// PLATFORM_URL=http://platform:8100 via docker-compose.yml env.
const PLATFORM_URL = firstNonEmpty(
  process.env.PLATFORM_URL,
  "http://localhost:8100",
);

const nextConfig = {
  // Expose the build-time version to the browser so the sidebar badge
  // can compare it against GitHub's latest release.
  env: {
    NEXT_PUBLIC_APP_VERSION: APP_VERSION,
    NEXT_PUBLIC_API_BASE,
    NEXT_PUBLIC_AUTH_ENABLED,
  },

  // Standalone output: self-contained server.js + minimal node_modules
  // This eliminates the need to copy the full node_modules into Docker production images
  output: "standalone",

  // Move dev indicator to bottom-right corner
  devIndicators: {
    position: "bottom-right",
  },

  // Transpile mermaid and related packages for proper ESM handling
  transpilePackages: ["mermaid"],

  // Turbopack configuration (used when running `npm run dev:turbo`)
  turbopack: {
    resolveAlias: {
      // Fix for mermaid's cytoscape dependency - use CJS version
      cytoscape: "cytoscape/dist/cytoscape.cjs.js",
    },
  },

  // Webpack configuration (used for production builds - next build)
  webpack: (config) => {
    const path = require("path");
    config.resolve.alias = {
      ...config.resolve.alias,
      "@": path.resolve(__dirname),
      cytoscape: path.resolve(
        __dirname,
        "node_modules/cytoscape/dist/cytoscape.cjs.js",
      ),
    };
    return config;
  },

  // Rewrites: proxy /api/platform/* → platform API.
  // The web frontend cannot call the platform container directly from
  // the browser.  Next.js server-side rewrites bridge the gap so
  // requests land on the same origin.
  async rewrites() {
    return [
      {
        source: "/api/platform/:path*",
        destination: `${PLATFORM_URL}/api/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
