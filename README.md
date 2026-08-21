# pi-triple-deps

**Pi-Triple 公用依赖仓库** —— 发布 `@away_from/shared` 与 `@away_from/infra`，是 [PTL](https://github.com/Phenol1145/pi-triple-ptl) 与 [PTH（FRACTA engine 当前代码名）](https://github.com/Phenol1145/pi-triple-pth) 共享的 npm 依赖层。

![node](https://img.shields.io/badge/node-%3E%3D22-green)
![tests](https://img.shields.io/badge/tests-73-brightgreen)
![version](https://img.shields.io/badge/version-1.5.0-blue)

- **定位**：配置/路径/work-mode/session/program-manifest/`execution/v1` 执行面协议等跨产品协议，以及平台探测/workspace/logger/model-router/sdk 适配/container-runtime。
- **导航**：Quick Start · [包目录](#包目录) · [开发](#development) · [仓库定位](docs/POSITIONING.md) · [文档索引](docs/README.md)

## ✨ Quick Start

消费方直接安装已发布包：

```bash
npm install @away_from/shared@^1.5.0 @away_from/infra@^1.5.0
```

本地开发：

```bash
git clone https://github.com/Phenol1145/pi-triple-deps.git
cd pi-triple-deps
npm install
npm run lint && npm run build && npm test   # 12 files / 73 tests
```

## 包目录

| 包 | 内容 | 主消费方 |
|----|------|----------|
| `@away_from/shared` | 配置/路径/work-mode/session/program-manifest/template-agents/tmux 等跨产品协议 | PTL · PTH |
| `@away_from/infra` | 平台探测/workspace/logger/model-router/sdk 适配/container-runtime | PTL · PTH |

版本同步原则：shared 与 infra 一起发版（当前 `1.5.0`）；破坏性协议变更先在主仓升级，再发布本仓。

## What it can do

- **协议单一真相源**：`program-manifest`、`work-mode`、`session`、`execution/v1` 等类型只在此仓定义，PTL/PTH 各自依赖同一 npm 版本。
- **SDK 适配边界**：所有对 `@earendil-works/pi-coding-agent` 的 import 收敛在 `infra/sdk-adapter`，SDK 升级只改一处。
- **自包含模板**：`shared` 包内携带 `docs/ptl/templates/AGENTS.md.tpl`，npm 安装后模板路径不依赖仓库根。

## Architecture

```
pi-triple-deps
├── packages/shared     # 协议/config/session/tmux/template-agents（+ 包内 docs 模板）
├── packages/infra      # platform/workspace/logger/model-router/sdk-adapter/container-runtime
├── test/               # shared-barrel + 8 个纯 deps 单测
├── vitest.config.ts    # alias 直连 src（无需先 build）
└── tsconfig.base.json  # 与主仓同源
```

## Development

```bash
npm run lint          # 两包 tsc --noEmit
npm run build         # shared → infra
npm test              # vitest，无 Docker 依赖
```

## Roadmap

- ✅ v1.5.0：从主仓 filter-repo 拆出；shared 模板自包含 + `./tmux` 子路径导出
- 🚧 GitHub Actions 门禁在 npm 包真实发布后自动生效

## Documentation

- [Phase 1 拆仓报告（主仓归档）](https://github.com/Phenol1145/pi-triple/blob/main/docs/pth/phase1-deps-split-report.md)
