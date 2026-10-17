# 开发、线上验证与正式发布

固定顺序：`功能分支 → PR → dev → 线上验证 → PR → main → 正式标签/Release`。

## 开发与候选构建

从最新 `origin/dev` 创建专属功能分支，完成验证后向 dev 提 PR。合并前重新获取远端目标分支，处理过期基线与冲突，再运行受影响的验证。

dev 的每次推送触发 CI 和 `dev-build`。构建产物为 Actions artifact `dev-linux-amd64-<完整SHA>`，包含 zip、SHA256SUMS 和 SOURCE.json。候选程序带 `dev-<12位SHA>` 版本。将正式 Release 更新中心用于稳定版本；dev 候选从对应构建下载，由协调者进行受控部署。

## 先部署指定 dev 提交

1. 记录 dev 完整 SHA 和 Actions run，确认 CI、构建成功，核对 SOURCE.json 和 SHA256SUMS。
2. 备份数据库、现有程序和配置/密钥，记录任务原有控制状态、其他容器和业务实体数量。
3. 正式暂停需要维护的任务，等待模型、工具、PTY、MCP 和浏览器真实退出。陈旧引用只能在证明旧执行退出后通过正式恢复路径处理。
4. 以既有容器持久程序机制安装候选，不重建 PostgreSQL 或其他服务。核对运行中程序 SHA、版本、公网及业务 API、配置指纹。
5. 仅恢复维护中暂停的任务，验证 Worker 真正领取并执行意图、终态正常写入，以及相关功能实际行为；保存回滚材料和脱敏证据。
6. 验收失败先恢复服务或回滚，在 dev 修复后重新部署。成功记录只绑定这次实际验收的完整 SHA。

正常重启会保留原任务控制状态，等待 Planner、Worker、主 Agent 和出口回收真实结束；启动后自动唤醒原已准入且未暂停的任务。启动期间模型或代理尚未恢复时，Planner 保留触发并退避重试。Compose 的应用停止宽限为 2 分钟，运维命令不要缩短该等待窗口。

强制终止、断电或清理失败留下的旧活动引用仍按正式恢复流程处理。数据库会话消失不能证明浏览器、工具或外部租约已经退出，禁止通过清引用或改任务状态绕过这一保护。

只有上述验收成功后，执行以下 GitHub 登记。将 `<已验收dev完整SHA>`、`<候选Actions运行地址>` 和 `<脱敏验收证据地址>` 替换为真实值：

```powershell
$request = @{
  ref = '<已验收dev完整SHA>'
  environment = 'production-dev'
  auto_merge = $false
  required_contexts = @()
  transient_environment = $false
  production_environment = $true
  description = '指定 dev 提交已完成线上验收'
} | ConvertTo-Json
$deployment = ($request | gh api repos/moeacgx/ARTEX-P/deployments --method POST --input - | ConvertFrom-Json)
@{
  state = 'success'
  auto_inactive = $false
  environment_url = 'https://artex.sn.ink'
  log_url = '<脱敏验收证据地址>'
  description = '程序 SHA、业务 API 与真实 Worker 行为验收通过'
} | ConvertTo-Json | gh api "repos/moeacgx/ARTEX-P/deployments/$($deployment.id)/statuses" --method POST --input -
```

部署开始/失败也可创建 in_progress/failure 状态；失败记录不能作为晋级依据。不得仅因编译成功或 health 200 就登记 success。

## 晋级 main 与发版

从 dev 向 main 创建 PR，在描述中注明已验收的完整 SHA、候选构建和线上证据。`branch-policy` 校验来源及当前 SHA 的成功部署记录，`checks` 执行基础验证。

main PR 通过后使用 `gh pr merge <编号> --merge` 保留 dev 的提交链。不要删除 dev 分支。功能分支合并后由 GitHub 自动删除。

晋级 main 后，从最新 main 打正式标签。`release` 工作流确认标签提交被 main 包含，再发布五平台 zip、SHA256SUMS 和双架构镜像。正式程序版本由标签注入，dev 候选与正式程序的版本标识不同，应分别核对各自产物 SHA。

main 合并完成后，将 main 的合并提交同步回 dev（通过同步 PR；无功能改动）。新功能继续从最新 dev 创建。dev 如有新提交，不能复用较早提交的线上成功记录。

## 已归档待办

旧 PR #1 的 API Key 自动填充修复未在本次分支整理中合入。完整提交 `bb8fb13fb5b831e85cd378f9afc4fba7d199026d` 已归档；继续时从 dev 建新分支，重新审查与验证后提 PR。旧工作树和未提交检查脚本保留。
