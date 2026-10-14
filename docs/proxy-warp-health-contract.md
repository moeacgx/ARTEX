# WARP 持续检测与安全恢复合同

适用基线：ARTEX-P 6dc3c3e（v2.2.11）。WARP owner 仅维护 egress/warp.go、egress/warp_health*.go、db/warp_health.go 及专属测试、server/warp_health.go 及专属测试、本文件。共享入口由后端整合 owner 串行接线；本专属提交不代表共享入口或生产已验收。

## DB 准入与意图接口

以下 Tx helper 只复用传入事务，不另借连接，使用与代理管理相同的 proxyLifecycleLock。

~~~go
func (d *DB) MigrateWarpHealth(ctx context.Context) error
func (d *DB) WithWarpHealthLifecycle(ctx context.Context, fn func(*sql.Tx) error) error
func (d *DB) WarpHealth(ctx context.Context, id int64) (WarpHealthState, error)
func CheckWarpHealthAdmissionTx(ctx context.Context, tx *sql.Tx, proxyID int64) error
func AcquireWarpHealthActivityTx(ctx context.Context, tx *sql.Tx, proxyID int64, activityToken string) error
func ReleaseWarpHealthActivityTx(ctx context.Context, tx *sql.Tx, activityToken string) error
func CheckWarpHealthMutationTx(ctx context.Context, tx *sql.Tx, proxyID int64) error
func SyncWarpHealthIntentTx(ctx context.Context, tx *sql.Tx, proxyID int64, intent WarpHealthIntent) error
func SetWarpAutoRecoveryTx(ctx context.Context, tx *sql.Tx, id, expectedRevision int64, enabled bool) error
func WarpHealthIdleTx(ctx context.Context, tx *sql.Tx, id int64) error

type WarpHealthIntent struct {
    Desired bool
    Material string
    ProfileRevision uint64
}
type WarpHealthClaim struct {
    ProxyID int64
    Token string
    Revision int64
    Kind string
}
type WarpHealthRepairGuard func(context.Context, *sql.Tx, int64) error
~~~

WarpHealthState 的 ProxyID/Revision 为 int64，AutoRecoverEnabled 为 bool；另含 Desired、Imported、Material、ProfileRevision、State、EgressIP、Failures、RecoveryAttempts、时间字段、Claim/ClaimState、OwnerHost/OwnerPID。Owner 仅用于保守确认旧管理进程退出，不进入 DTO。

proxyID=0 的准入/登记 helper 直接返回 nil；普通代理仍检查启用和可用状态。WARP 同时检查明确 desired、完整材料、pending 和 repair/unknown 屏障。probe claim 允许真实活动准入，repair 升级时在同一锁下重新复核空闲。

每次真实操作必须生成新的 UUID。重复登记报冲突；迟到回调只删除原 token。context 取消不能提前释放活动，须在底层真实操作退出的 defer 中解除。静态 task、conversation 或 llm_profile 引用不能替代真实活动。

健康维护开关的唯一权威是 warp_proxy_health.auto_recover_enabled，默认 true，不与 settings 双写。它与 Desired 分离：false 仍探测，Desired=false 绝不自动启动。开关 CAS 不抢占 repair/unknown；成功只递增 revision、作废旧 probe，不改变 Desired。expected_revision 必须为 1 到 9007199254740991 的整数。缺行返回 sql.ErrNoRows，旧/不安全 revision 返回 ErrWarpHealthStale，屏障或版本耗尽返回 ErrWarpHealthBlocked。

## 控制器与运行器接口

~~~go
func NewWarpHealthController(store *db.DB, runtime WarpHealthRuntime, policy db.WarpHealthPolicy) (*WarpHealthController, error)
func (c *WarpHealthController) Start(ctx context.Context) error
func (c *WarpHealthController) Close(ctx context.Context) error
func (c *WarpHealthController) RunOnce(ctx context.Context, proxyID int64) error
func (c *WarpHealthController) ImportProfile(ctx context.Context, proxyID int64) error
func (c *WarpHealthController) SyncProfile(ctx context.Context, proxyID int64) error
func (c *WarpHealthController) ResolveUnknown(ctx context.Context, proxyID int64) error
func (c *WarpHealthController) DTO(ctx context.Context, proxyID int64) (*WarpHealthDTO, error)
func (c *WarpHealthController) SetAutoRecoveryAllowed(fn func(context.Context, int64) (bool, error))
func (c *WarpHealthController) SetRepairGuard(fn func(context.Context, *sql.Tx, int64) error)

type WarpHealthRuntime interface {
    HealthMaterial(string) (egress.WarpProfile, string, error)
    Get(string) (egress.WarpProfile, error)
    Probe(context.Context, string) (egress.WarpProfile, error)
    Reconnect(context.Context, string, string) (egress.WarpProfile, error)
    ConfirmStopped(context.Context, string) error
}
~~~

构造不启动 goroutine，不访问公网，不注册。WarpManager 实现此 runtime。HealthMaterial 返回完整注册/配置/认证材料的 SHA256 摘要；不要自行拼造。Get 只读取本地快照，不得加入网络或数据库调用，因为 DTO 在短生命周期事务内同时复核 DB 屏障、proxy/pending 与本地 runtime 状态。

SetAutoRecoveryAllowed 默认允许，只是额外否决；false 或读错推迟 repair，探测照常。SetRepairGuard 在升级 repair 的同一短事务内权威复核，必须只使用传入 tx。人工 ResolveUnknown 与手动 claim 也调用相同 guard。

后端整合 owner 在 guard 中检查 proxy_subject_activity(token,policy_id,revision,proxy_id,backend_pid,kind)，及 pending/applying/recovery_required、switch_token/reserved_proxy_id 等主体真实活动和预留。专属内建检查覆盖 warp_proxy_health_activity、task_proxy_activity、task_operation_activity；这些不代替主体根/最终 LLM 的新活动表。group 不能通过 abort 逃逸；ClearStale 只能走隔离管理恢复。

## 手动 WARP action 的明确 claim 接口

手动 start/recover/stop 使用以下接口，不再用长事务包网络，也不要仅靠 CheckMutation 后锁外操作。

~~~go
func (d *DB) TryWarpHealthExecution(ctx context.Context, id int64) (*WarpHealthExecution, error)
func (e *WarpHealthExecution) Close() error
func (e *WarpHealthExecution) BeginWarpHealthManual(ctx context.Context, id int64, guard WarpHealthRepairGuard) (WarpHealthClaim, error)
func (e *WarpHealthExecution) CompleteWarpHealthManual(ctx context.Context, claim WarpHealthClaim, intent WarpHealthIntent, finish func(*sql.Tx) error) error
func (e *WarpHealthExecution) MarkWarpHealthUnknown(ctx context.Context, claim WarpHealthClaim) error

func BeginWarpHealthManualTx(ctx context.Context, tx *sql.Tx, id int64, guard WarpHealthRepairGuard) (WarpHealthClaim, error)
func CompleteWarpHealthManualTx(ctx context.Context, tx *sql.Tx, claim WarpHealthClaim, intent WarpHealthIntent, finish func(*sql.Tx) error) error
~~~

手动 claim 复用 claim_kind=repair 的独占屏障，不累加自动恢复次数，也不改自动维护开关。它允许明确启动 stopped 档案，不要求自动维护开启；仍拒绝 pending、既有 claim、repair/unknown 和真实活动/切换/预留。Tx 版本不自行持有执行 session，使用它的调用者仍必须在整个真实操作期间持有相应 WarpHealthExecution。

接线顺序：

1. 获取 TryWarpHealthExecution。try-lock 失败返回忙，不能按超时抢占。
2. 用执行对象 BeginWarpHealthManual，提交短事务和原 token/revision。
3. 注册 context.AfterFunc：取消/超时时用独立清理 context 调用 execution.MarkWarpHealthUnknown。标记完成后保留原 claim，不先释放 session。
4. 锁外运行原 WarpManager action，同步等待真实函数及底层进程操作退出。即使请求已取消，也不能先返回并启动替代操作。
5. 如果取消/超时或 ErrWarpExitUnknown，保留 unknown，禁止迟到完成。正常返回后在锁外读取 runtime.HealthMaterial，形成真实 Desired/摘要/ProfileRevision。
6. execution.CompleteWarpHealthManual 的 finish(tx) 只在传入事务中写代理连接/状态；不得另借连接或执行网络。helper 先核对原 token/revision/active，再执行 finish，清 claim、SyncWarpHealthIntentTx 和 revision 一起提交。finish 失败共同回滚；调用者将仍匹配的 claim 标记 unknown。
7. 在所有真实执行退出后关闭 execution。不要再另做独立 SyncProfile 来替代上述原子提交。

CompleteWarpHealthManual 后健康状态保守回到 unknown/stopped，等待后续真实探测再给有效 IP。代理状态先由 finish 写入，显式 start 应在同一事务把旧 stopped 改为当前运行结果，再同步意图；否则 DB 明确 stopped 仍会优先。手动 stop 则由 finish 写入 stopped，并传入 Desired=false；不要从当前 DB healthy 或 action 名称推断意图。

## Schema、执行 session 与 unknown 恢复

warp_proxy_health 保存明确意图、导入标志、维护开关、材料摘要、profile_revision/revision、状态/有效 IP/固定错误、失败/恢复次数、检测/恢复时间、claim token/kind/state、last_probe_token、owner_host/owner_pid。迁移保守插入，不从 healthy 推断 Desired；Owner 字段有幂等补列。warp_proxy_health_activity 用每次操作 UUID 为主键，proxy FK 限制删除。

每个档案有独立 session advisory 执行锁。执行连接由实际已打开的 pgx 配置复制创建，不读取后来改变的环境 DSN；不占业务池容量。持锁期间健康短事务复用该连接，网络与进程等待在事务外。业务池 MaxOpenConns=1 时仍能做查询、准入和取消标记。档案锁外完成 probe、启动监听及 TLS 等待；不同档案不持全局 runtime 网络锁。

claim 无租期，未知屏障不会自动到期解封。执行函数取消/超时先标记 unknown，底层即使迟到成功也不能清屏障。controller.Close 取消循环及直接 RunOnce/ResolveUnknown，等待真实操作；超时只报告未退出，不释放其 session 或 claim。

正式人工恢复只能调用 controller.ResolveUnknown(ctx,id)，不接受 exitedConfirmed 或 HTTP 布尔证明。它必须：

1. try-lock 获取原档案执行 session，成功才有机会确认旧执行已结束。
2. 核对原 token/revision、宿主/管理 PID 与当前宿主真实执行登记；跨宿主、未知 Owner、仍存活的其他管理 PID 保持屏障。
3. 同生命周期锁检查真实活动/切换/预留空闲，再在短事务外 ConfirmStopped。
4. ConfirmStopped 只停止本 Manager 明确拥有的进程并等待 cmd.Wait；其他 Manager 遗留 PID 只能由真实 OS 退出状态证明已经退出，不盲杀。缺失/不完整启动记录保持 unknown。
5. 同一执行 session 内再次复核活动/guard 和原 token/revision，原子解封并递增 revision。不会在该路径注册、生成或自动启动。

## 默认行为与 HTTP 接线

默认策略固定：每 60 秒真实 TLS、带认证 SOCKS 的 Cloudflare Trace；必须同时验证公网 IP 与 warp=on/plus，拒绝重定向、超长、重复字段。失败立即清 IP，三连失败先原地复探，安全空闲才复用同注册材料重连。恢复间隔为 300/600/1200/2400/3600 秒并封顶；同 IP 也算恢复，不保证换 IP。

ImportProfile 仅在未导入时读取磁盘明确 Desired、RegistrationAttempted/Registered/Generated、材料文件及认证配置。数据库明确 stopped 优先。缺档案、未知注册、pending、材料不全均不能自动启动。健康路径不调用 Create/register/generate。

共享 HTTP 由整合 owner 接线：

- GET /api/proxies/{id}/health-policy：200 返回 proxy_id（数字）、revision、auto_recover_enabled、probe_interval_seconds=60、failure_threshold=3、recovery_min_seconds=300、recovery_max_seconds=3600。
- PUT 同地址：输入 expected_revision、auto_recover_enabled。旧 revision 返回 409 warp_health_revision_conflict 与当前 revision；JSON 安全整数由 HTTP 解码层与 DB CAS 双重校验。
- POST /api/proxies/{id}/health/recovery：请求体 {}，只调用 ResolveUnknown。409 recovery_busy/recovery_required；成功 200 runtimeProxyDTO。不能接受用户布尔解封。
- 既有 proxy DTO 增加 warp_health，保留 warp_phase/egress_ip 字段；有效 IP 同样受健康屏障约束。

WarpHealthDTO 只含 proxy_id、revision、auto_recover_enabled、desired_running、state、连续失败/恢复次数、检测/恢复时间、egress_ip、固定 last_error。state：unknown/stopped/disabled/pending/checking/ready/failed/deferred/repairing。只有 DB healthy/enabled/desired，且 runtime 真实 ready、进程在线、IP 校验通过时才给 IP；unknown/failed/deferred/repairing 不给有效 IP。DTO 不暴露材料摘要、claim、Owner、私钥、认证或工具原始错误。

## 本轮独立验证证据

专属测试使用新建本地隔离 PG、合成 TLS/SOCKS 和本地子进程。没有公网注册、旧库清理、Vault、生产、推送或其他 Agent 修改。

本轮容器：

- artex-warp-owner-sol-pg-20261014-1528：postgres:16-alpine；本机 127.0.0.1:3896；artex_warp_sol_test。
- artex-warp-owner-sol-regression-pg-20261014：postgres:16-alpine；容器内 127.0.0.1:25432；artex_proxy_verify_test，另建空白 artex_proxy_test 用于 overlay 兼容验证。
- Linux Go 运行于一次性 golang:1.26.1-bookworm 容器，GOTOOLCHAIN=auto 实际下载并使用 Go 1.26.3；源树只读挂载。

最终源码/测试冻结后实际运行：

~~~powershell
# 仅给新建专属 PG；三个包 TestMain 也指向同库。
$env:ARTEX_WARP_HEALTH_TEST_DSN='postgres://artex_warp_test@127.0.0.1:3896/artex_warp_sol_test?sslmode=disable'
$env:ARTEX_PG_DSN=$env:ARTEX_WARP_HEALTH_TEST_DSN
go test -count=1 -v -timeout 60s ./db ./egress ./server -run 'WarpHealth|^TestWarpCreate|^TestWarpRejects'
go build ./...
go vet ./db ./egress ./server
go vet -copylocks=false ./db ./egress ./server
~~~

Windows Go 1.26.3/386：健康及原不确定注册回归 39 个顶层测试 PASS，无 SKIP；db 14、egress 13（包含子进程入口）、server 12。Linux Go 1.26.3/amd64 使用相同筛选加 -race：三包 PASS，无竞态、无 SKIP。全部测试 timeout=60s；合成子进程自身 timeout=45s。

覆盖：单连接独立 session/事务、两 Manager、取消及迟到 claim、真进程退出/Wait、未知材料/Owner、手动 stopped、维护关闭仍探测、CAS 与 repair 真并发、人工恢复活动复核、DTO 脱敏与一致性、同 IP 原地/重连恢复、六档退避、原 UUID pending/执行 token/replay/异载荷/删除墓碑、手动 claim 原子完成/回滚。

go build ./... PASS。原样 go vet 有一个基线既有 FAIL：server/channel_proxy_reference_test.go:73 复制含 RWMutex 的 Task。该文件工作区 blob 与基线均为 e9b251f4558384610b16978a62218cc52cf45525；超出 WARP 写边界，未修改。关闭这一基线 copylocks 检查后其余 vet PASS，不能宣称原样 vet 全绿。

原样 Linux TestProxyPGWarpCreateIdempotency FAIL：既有合成 Trace 用 198.51.100.42，新的严格公网 IP 校验正确拒绝该文档地址，首次响应 502。未改共享测试文件、未放宽公网校验。临时 Go overlay 仅把 proxy_warp_linux_test.go 的 9 处合成/期望 IP 改成 8.8.8.8，不改变 HTTP/UUID/墓碑断言或生产实现。overlay 在新空白 artex_proxy_test 上运行以下原测试，全部 PASS、无 SKIP：

~~~sh
go test -overlay /overlay/overlay.json -count=1 -v -timeout 60s ./server \
  -run '^TestProxyPGWarp(Create|OneClick|RuntimeRecovery|ActionSingleConnection|ChannelDeletePreservesProfile|DeleteRunning)'
~~~

覆盖 10 个原顶层 HTTP 测试：UUID 幂等、并发 Manager、orphan pending、载荷白名单、失败重启、一次创建链、原地恢复/重复 start、单连接 start/stop、渠道引用删除保护、运行时删除。整合 owner 应将原共享合成测试的文档 IP fixture 更新为有效公网值，再对最终共享接线做原样测试；overlay PASS 不等于原样共享测试 PASS。

专属代码已经实现并验证；共享入口、主体根/最终 LLM 全路径准入、手动 action finish 接线及最终 HTTP 路由仍由后端整合 owner 完成，不在本专属树写入。
