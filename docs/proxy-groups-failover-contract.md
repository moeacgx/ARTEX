# 代理分组、主体策略与故障切换合同

后端基线：`6dc3c3e`。本合同供前端和整合 owner 接线；实现与验证尚在进行。
WARP 健康 DTO 由独立 owner 提供，不在本文猜测字段。新增组与主体策略 ID 使用字符串正整数；既有 TaskDTO.proxy_id 与 ProxyDTO.id 保留数字。版本使用 JSON 非负安全整数；时间为 RFC3339 或 null。代理成员只引用档案 ID，不复制凭据，响应沿用脱敏 ProxyDTO。

## 分组接口

- `GET /api/proxy-groups` → `200 {"groups":[ProxyGroupDTO]}`。
- `POST /api/proxy-groups`，请求 `{name,enabled}` → `201 ProxyGroupDTO`；enabled 缺省 true。
- `GET /api/proxy-groups/{id}` → `200 ProxyGroupDTO`。
- `PUT /api/proxy-groups/{id}`，请求 `{expected_revision,name,enabled}` → `200 ProxyGroupDTO`。
- `DELETE /api/proxy-groups/{id}?expected_revision=N` → `204`。仍被主体授权引用时返回 409。
- `PUT /api/proxy-groups/{id}/members`，请求 `{expected_revision,proxy_ids:["1","2"]}` → `200 ProxyGroupDTO`。数组是完整有序替换，空数组清空，重复或无效 ID 返回 400；移除正在使用或预留的成员返回 409。

`ProxyGroupDTO = {id,name,enabled,revision,members:[{proxy_id,position,proxy:ProxyDTO}]}`，position 从 0 开始。组名 1–80 个字符，组 ID 数组最多 100 项，成员最多 500 项。
Resin 可以出现在档案列表，但 group 候选返回 `supported:false,unsupported_reason:"group_resin_lease_scope_unsupported"`；对话 Resin 返回 `conversation_resin_lease_scope_unsupported`。UI 应显示原因。

## 任务和对话策略

- `GET /api/tasks/{id}/proxy`、`GET /api/conversations/{id}/proxy` → `200 ProxyPolicyDTO`。
- `PUT /api/tasks/{id}/proxy`、`PUT /api/conversations/{id}/proxy`，必须含 `{expected_revision,proxy_mode}`。人工修改仅在真实活动和资源清理全部退出后允许，暂停或终态不代表空闲。
- `proxy_mode` 为 `inherit | direct | fixed | group`。fixed 必须含 `proxy_id`（任务 PUT 为数字，对话 PUT 为字符串正整数）；其他模式只允许省略或 null。inherit 在保存事务中明确采样默认并持久快照，GET 只核对，不重新采样。
- group 可含 `allowed_group_ids:["1","2"]`、`agent_failover_enabled:boolean`、`failover_cooldown_seconds:整数(0–86400)`、`failover_max_switches:整数(0–1000)`。初次 group 保存必须给非空组数组；首次缺省 Agent 切换关闭、冷却 60 秒、预算 3 次。后续省略保留，显式 `[]` 清除授权，但 group 状态下空候选拒绝保存；null 不表示保留，返回 400。离开 group 可以显式 `[]` 清除组引用。
- 保存完整期望模式；只允许专属策略接口修改授权，通用任务/对话编辑和 Agent 管理工具不能赋予本主体使用权限。
- group 首次保存原子选定有效获准组并集中的合格成员。group 策略同时约束 LLM 和工具，渠道组外代理不能覆盖；耗尽返回 `proxy_candidates_exhausted`，不直连。

`ProxyPolicyDTO = {subject_kind:"task"|"conversation",subject_id,proxy_mode,proxy_id,proxy_revision,allowed_group_ids,agent_failover_enabled,failover_cooldown_seconds,failover_max_switches,authorization_revision,proxy:ProxyDTO|null,exit_verified_at:string|null,latest_switch:ProxySwitchRequestDTO|null,cooldown_until:string|null,remaining_switches:整数}`。

任务 PUT 保留既有任务详情响应（兼容其数字 `proxy_id`），同时增加上述策略字段；对话 PUT 和主体 proxy GET 返回 ProxyPolicyDTO，使用字符串 `proxy_id`。`ProxyDTO.id` 同样保留既有数字类型。组 revision 冲突为 `proxy_group_revision_conflict`；主体冲突保留 `proxy_revision_conflict`；真实占用保留 `proxy_busy`。错误响应沿用 `{error,code,...}`，409 主体冲突附 `current_revision`。未知或结果不确定时客户端仅 GET 对账。策略 DTO 另含 `recovery_required:boolean`，仅表示持久切换屏障需要管理恢复。

- `POST /api/{tasks|conversations}/{id}/proxy/policy-recovery`：新主体屏障的人工恢复入口，请求必须含 `expected_revision` 和 `action`。`retry` 只重新提交原持久目标，不重新采样 inherit、不重置预算、不重放业务工具。`replace_policy` 必须同时提供完整期望 `proxy_mode` 和相应策略字段，明确提交新的人工策略。原目标被撤权或禁用时可用此操作重新授予有效策略，旧请求记为 failed。成功 `200 ProxyPolicyDTO`。
- Resin 回收结果不明确时 `retry` 不再次 DELETE。人工核对远端旧租约已释放后可用 `confirm_released`，或 `confirm_released_replace_policy` 加完整新策略；它们先校验原材料身份再标记原记录 released，不允许覆盖旧材料。仍有真实活动、清理未退出或版本不匹配时均拒绝恢复。此入口没有 `abort_direct`，不能让 Agent 越过分组授权屏障；旧任务非分组恢复接口保持兼容。

## Agent 工具与异步状态

- `list_allowed_proxies` 输入 `{}`，主体来自正式执行 context。返回 `{policy:ProxyPolicyDTO,candidates:[{proxy_id,proxy:ProxyDTO,group_ids:[字符串],supported,unsupported_reason?}],failure_evidence:[{kind,reference}]}`。不接受模型传主体 ID。
- `request_proxy_failover` 输入 `{expected_revision,reason,evidence:{kind:"connection_failure"|"tunnel_failure"|"process_failure"|"health_confirmation",reference:"运行时证据ID"},target_proxy_id?:"正整数"}`。reason 1–500 字符；证据 reference 必须对应服务端记录并与本主体、出口、版本匹配。理由文字不产生授权。
- 返回 `{accepted:true,request:ProxySwitchRequestDTO,end_turn:true}` 后结束本轮，不在工具内等待自身活动计数。调度器在 LLM/工具/MCP/browser 及其清理全部退出后跨 Manager 独占应用。不得自动重放有副作用工具。
- 同一主体、base revision 只允许一个有效意图；重复请求返回原请求 ID。HTTP `POST /api/{tasks|conversations}/{id}/proxy/switch-requests` 使用同一请求体、人工登录上下文和证据校验，返回 `202`；此接口不隐式赋予切换权限。
- `GET /api/{tasks|conversations}/{id}/proxy/switch-requests/{request_id}` → `200 ProxySwitchRequestDTO`，请求必须属于该主体。

`ProxySwitchRequestDTO = {id,subject_kind,subject_id,base_revision,authorization_revision,state:"pending"|"draining"|"applying"|"succeeded"|"failed"|"recovery_required",from_proxy_id,to_proxy_id,reason,error_code,created_at,updated_at,completed_at:string|null,result_revision:整数|null}`。

预算按人工授权版本持久累计，重启或切换成功不重置。撤权、组禁用、成员变化在登记/执行/提交/每个新操作准入再次复核；政策失效拒绝网络操作。屏障无超时自动夺权；释放成功但提交失败保持 recovery_required，不能恢复旧运行或 group→direct 逃逸。403/验证码不构成全局基础设施失败，429 按 Retry-After 等待、不为绕限流轮换。

## 阶段实施计划

1. 数据层：分组 CAS/FK、主体策略和授权版本、对话活动、持久请求/预算/证据/独占屏障；网络放在短事务外，单连接路径不二次借连接。
2. HTTP/DTO：按本文实现组 CRUD、任务/对话策略及状态查询；专属策略接口拒绝 Agent 通用编辑扩权。
3. 执行层：正式 context 身份、活动准入/实际退出、工具请求即返并结束本轮、异步跨 Manager 调度、每次正式操作刷新快照和 provider/桥/路由/连接。
4. WARP 接线：待 owner 精确签名和 commit 后整合迁移、启动关闭、DTO、创建/人工 action/禁用/删除及统一准入，不重写健康算法。
5. 隔离验证：新 PostgreSQL 库；每项 Go 测试 `-timeout 60s`；双 Manager、撤权、真实 paused 活动、预算重启、候选耗尽、双对话出口、合成 HTTP/browser/录制、Resin uncertain 不二次 DELETE、单连接，以及 WARP UUID/墓碑/删除引用/任务追加回归。只合成，不真实 Cloudflare 注册。
6. 交付：本地提交源码与合同，提交 hash、验证日志、通过/失败/未测清单交主脑；不 push/PR/发版，功能完成后才冻结。
