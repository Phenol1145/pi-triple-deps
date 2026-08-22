#!/usr/bin/env node
/**
 * scripts/check-doc-links.mjs —— deps 仓库 docs 相对链接校验。
 *
 * 只校验仓库内相对链接（忽略 http/https/mailto/纯锚点）；任何目标缺失 → exit 1。
 * 与 pth/ptl 的 scripts/check-doc-links.ts 保持相同语义；deps 用纯 Node 实现避免引入 tsx。
 */

import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const MD_EXT = /\.md$/;

function walk(dir, onFile) {
  for (const name of readdirSync(dir)) {
    const full = resolve(dir, name);
    if (statSync(full).isDirectory()) walk(full, onFile);
    else onFile(full);
  }
}

function collectDocLinkIssues(roots) {
  const files = [];
  for (const scanRoot of roots) {
    const full = resolve(root, scanRoot);
    if (!existsSync(full)) continue;
    if (statSync(full).isDirectory()) walk(full, (file) => { if (MD_EXT.test(file)) files.push(file); });
    else if (MD_EXT.test(full)) files.push(full);
  }

  const issues = [];
  for (const file of files) {
    const text = readFileSync(file, "utf8");
    const lines = text.split("\n");
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const linkRe = /\[[^\]]*\]\(([^)]+)\)/g;
      let match;
      while ((match = linkRe.exec(line)) !== null) {
        const raw = match[1].trim();
        const clean = raw.split("#")[0].split("?")[0];
        if (!clean || raw.startsWith("http://") || raw.startsWith("https://") || raw.startsWith("mailto:")) continue;
        if (raw.startsWith("<") && raw.endsWith(">")) continue;
        const target = resolve(dirname(file), decodeURIComponent(clean));
        if (!existsSync(target)) {
          issues.push({ file: relative(root, file), line: i + 1, target: raw, reason: "missing" });
        }
      }
    }
  }
  return issues;
}

const issues = collectDocLinkIssues(["docs", "README.md", "ARCHITECTURE.md", "TODO.md"]);
if (issues.length > 0) {
  for (const issue of issues) console.error(`❌ ${issue.file}:${issue.line} → ${issue.target} (${issue.reason})`);
  console.error(`doc links: ${issues.length} broken`);
  process.exit(1);
}
console.log("✅ doc links: all relative targets exist");
