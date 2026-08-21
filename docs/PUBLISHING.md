# 维护者发布流程（pi-triple-deps）

> 面向维护者；公开 README 不包含发布步骤。

## 前置

- npm 账号对 `@away_from` 范围有发布权限
- npm 需要 **Granular Access Token**（Web 登录 token 直接 publish 会被 403 拒绝）
- 版本同步原则：`@away_from/shared` 与 `@away_from/infra` 一起发版

## 发布

```bash
npm ci
npm run lint && npm run build && npm test

# 本地产物 + sha256（可选）
npm run pack:tgz

# 发布（在根目录执行；npm v11 需要 ./ 前缀）
npm publish ./packages/shared --access public
npm publish ./packages/infra --access public
```

## 发布后

- 确认 `npm view @away_from/shared version` 与 `npm view @away_from/infra version`
- 在消费仓（pi-triple-pth / pi-triple-ptl）执行：

```bash
npm ci && npm run lint && npm run build && npm test
```
