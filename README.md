# pi-triple-deps

Pi-Triple 公用依赖仓库，发布两个 npm 包：

| 包 | 内容 | 主消费方 |
|----|------|----------|
| `@away_from/shared` | 配置/路径/work-mode/session/program-manifest 等跨产品协议 | PTL · PTH |
| `@away_from/infra` | 平台探测/workspace/logger/model-router/sdk 适配/container-runtime | PTL · PTH |

## 开发

```bash
npm install
npm run lint && npm run build && npm test
```

## 发布（需 npm 登录）

```bash
npm run pack:tgz                              # 本地 tgz + sha256
npm publish packages/shared --access public
npm publish packages/infra --access public
```

版本同步原则：shared 与 infra 一起发版（当前 1.5.0）；破坏性协议变更先升级主仓再发布。
