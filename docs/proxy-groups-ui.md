# 代理分组、主体出口策略与 WARP 前端交付

工作树：`artex-proxy-groups-ui`；分支：`feat/proxy-groups-ui`；基线：`6dc3c3e`。
唯一写入范围为 `web/**` 与本文。未修改 Go、后端、TG、Vault、生产或凭据；未 push、发布或占用旧的 4173/18787/18879 服务。

## 页面与行为

- 代理中心增加分组目录与 Dialog 管理：名称、启用、搜索现有代理、按 ID 有序替换成员、多组成员、CAS 删除。名称与启用保存、成员保存分别使用当前明确版本，避免两次隐式写入。
- 管理分组仅维护目录；使用分组的授权位于任务/对话的独立出口策略 Dialog。任务详情使用 `TaskProxyControl` 包装共享组件，对话页头使用同一组件。
- 策略保留 inherit/direct/fixed，增加 group；可显式选择获准组、Agent 故障切换许可、冷却和次数预算。打开时读取权威 revision；首次 Agent 切换不会默认开启。
- group 首次保存由服务端原子选成员；前端不从草稿推断实际出口。展示当前代理、有效 IP 与验证时间、冷却剩余、剩余预算、最近切换原因和 pending/draining/applying/succeeded/failed/recovery_required 状态。
- 202 只表示受理；轮询只 GET 原请求和主体策略，不重发切换。组外成员、Resin 组候选与对话 Resin 禁选；候选耗尽不回落直连。
- 409 保留草稿与打开时 revision；500/断网/回读失败进入只读核对。明确核对并采用新版本后才能新提交。提交同步锁防止同事件循环双击；关闭、卸载与迟到 GET 有序号保护。
- 人工策略恢复使用独立 Dialog 和正式 policy-recovery 接口，权威 `recovery_required` 即使没有最近请求也阻止普通保存并隐藏有效 IP。retry 只恢复原持久快照，不重新采样 inherit、不重置预算、不重放业务工具；replace_policy 提交完整人工策略。Resin 的 confirm_released 两种操作须人工核对远端原租约已释放，不二次 DELETE、不提供 abort_direct、不发送退出自证。
- WARP 健康展示使用正式 `warp_health`；failed/unknown/deferred/repairing/pending 等状态隐藏有效 IP。没有服务端历史字段时不自行存历史 IP。
- 自动恢复开关通过 GET/PUT health-policy CAS；与 desired_running 分离。关闭仍探测，desired=false 不自动启动。默认探测 60 秒、连续失败 3 次，恢复 300–3600 秒有界退避。
- 人工恢复经 Dialog 明确提交空对象 POST；不发送 exitedConfirmed、token 或 revision 自证。服务端检查真实退出和空闲。未知/冲突后只 GET 核对，恢复复用原材料，不保证不同 IP。
- WARP 创建仍持有原稳定 UUID；pending/未知结果只 GET 原条目或目录，不自动重发创建/注册、不隐式产生新 key。运行健康与创建意图持久完成分别看待。
- 403/验证码与 429 文案明确为目标站点限制；不授权通过轮换绕过，不自动重放有副作用操作。Resin 租约确认改用现有 Dialog，页面无 window.confirm/alert。

## HTTP 接线

最终分组/策略合同完整只读自后端树 `docs/proxy-groups-failover-contract.md`。
WARP 默认健康文档完整只读自 WARP 专属树 `docs/proxy-warp-health-contract.md`；开关和人工恢复按主脑转发的最终 HTTP 签名接线。

`proxy-policy-api.ts` 封装 `/api/proxy-groups` CRUD、members CAS、任务/对话 GET/PUT proxy、POST switch-requests、GET 原请求与 POST policy-recovery。任务 PUT 返回完整 TaskDTO（数字 proxy_id），组件通过主体 GET 回读字符串 ID 的 ProxyPolicyDTO；恢复成功同样必须 GET 核对屏障已解除。
`warp-health-api.ts` 封装 GET/PUT `/api/proxies/{id}/health-policy` 和 POST `/api/proxies/{id}/health/recovery`。
生产组件默认使用真实 HTTP adapter，未将合成数据或 fixture 导入生产代码。新建任务的通用接口未获得 group 创建合同，因此不猜字段或在创建后隐式追加授权；在任务详情明确保存 group 策略。

## 本地验证

在 `web` 内运行：

```powershell
node scripts/verify-proxy-groups-tests.mjs
node node_modules/typescript/bin/tsc --noEmit --incremental false
$env:NEXT_PUBLIC_MOCK='0'
$env:NEXT_EXPORT='1'
node node_modules/next/dist/bin/next build
```

78 项行为/API 回归通过；整组测试由 runner 强制最多 60 秒。覆盖双击、CAS、未知只读、202、Resin/组外候选、有效 IP、稳定 WARP UUID、保留条目、独立持久屏障与人工策略恢复。
`tsc` 通过；本次修改文件的 Biome lint 与格式检查无 error。格式器只处理本轮修改文件，Git 差异没有无关整页内容改写。
非 mock 静态构建通过，32 个页面。产物来自本工作树；验证服务使用独立 `127.0.0.1:19431`。

```powershell
node scripts/verify-proxy-groups-server.mjs
npx --yes --package @playwright/cli playwright-cli -s=artex-proxy-groups-ui open http://127.0.0.1:19431/__fixture/bootstrap
npx --yes --package @playwright/cli playwright-cli -s=artex-proxy-groups-ui run-code --filename scripts/verify-proxy-groups-browser.mjs
npx --yes --package @playwright/cli playwright-cli -s=artex-proxy-groups-ui goto 'http://127.0.0.1:19431/function/tasks/detail/?id=12'
npx --yes --package @playwright/cli playwright-cli -s=artex-proxy-groups-ui run-code --filename scripts/verify-proxy-policy-browser.mjs
npx --yes --package @playwright/cli playwright-cli -s=artex-proxy-groups-ui run-code --filename scripts/verify-proxy-policy-stale-browser.mjs
npx --yes --package @playwright/cli playwright-cli -s=artex-proxy-groups-ui run-code --filename scripts/verify-proxy-policy-recovery-browser.mjs
```

Chromium 桌面 1440×1000、移动 390×844 的分组、WARP、任务/对话策略、取消/迟到响应及人工恢复验证通过。恢复弹窗额外检查内部 scrollWidth 与边界，已修正 fieldset 最小宽度导致的内部裁切，截图已复查。
浏览器日志的 409/500 为主动注入的冲突与未知结果。SSE 因合成服务不提供持续会话而显示重连，不计为真实执行验证。
日志、请求记录、截图和完整文件 SHA256 存于 `web/output/playwright/proxy-groups/`，由 Git ignore 排除验收产物，代码与本文单独本地冻结。`source-sha256.json` 覆盖全部修改文件，`build-sha256.json` 覆盖完整静态构建，`evidence-sha256.json` 覆盖日志、截图与请求记录，`delivery.json` 记录本地 commit 与验证层。

整合树另提供 `scripts/verify-proxy-groups-real.mjs`。它不内置 token、密码、数据库地址或代理凭据，只从测试环境读取合成身份和 ID；真实 Chromium 经 Next 非 mock 页面调用 Go API，并在独立 PostgreSQL 中核对分组成员与任务策略。2026-10-15 的隔离验收创建组 `#6`、保存成员 `#1/#2`，任务策略从 direct 保存为 group revision 1，再保存为 direct revision 2；截图为 `web/output/playwright/proxy-groups-real/ui-go-pg-direct.png`。

## 分层验收与待办

1. 纯交互状态与真实 API 客户端：已通过。
2. 真实 Chromium → 非 mock UI → 独立合成 Node HTTP：已通过。
3. UI → 真实隔离 Go → 新 PostgreSQL：已通过。验证层为 `Chromium → Next UI → Go API → artex_proxy_ui_verify_test`，使用合成身份、回环代理档案和终态空闲任务；没有生产凭据或外网代理操作。
4. 真实 Cloudflare 注册、生产登录、生产部署：不在本轮范围。

原创建意图的未知结果只 GET 核对，不自行解除屏障或按运行 healthy 推断持久意图完成。主脑转发的后端真实 PG 测试状态仅记为后端 owner 证据，不替代此 UI 的真实 Go/PG 联调，也不据此声明 MCP/browser/WARP 全面通过。
