# 有贝维格表数据更新脚本

1. 需要把 pusher.ts 中的 vikaToken 填充为开发者的 Token
2. 需要运行 [aktools](https://aktools.akfamily.xyz/) 服务,并把 aktoolsURL 填充为 aktools 服务的 URL。汇率和部分数据从 aktools 获取
3. 每天的 0 / 8 / 16 点需要更新一次数据 （UTC+8）

```bash
bun i
bun run dev
```
