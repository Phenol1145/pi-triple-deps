import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@away_from/shared": fileURLToPath(new URL("./packages/shared/src/index.ts", import.meta.url)),
      "@away_from/infra": fileURLToPath(new URL("./packages/infra/src/index.ts", import.meta.url)),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["test/**/*.test.ts", "packages/*/test/**/*.test.ts"],
    testTimeout: 90_000,
    maxWorkers: 4,
    poolOptions: { maxWorkers: 4 },
  },
});
