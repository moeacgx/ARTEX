import assert from "node:assert/strict";
import test from "node:test";
import { allowedCandidates, draftFromPolicy, effectiveExit, groupUnavailable, policyInput, policyMatches, proxyUnavailable, RevisionSubmission, cooldownRemaining, policyRecoveryInput, policyRecoveryMatches, policyRecoveryRequired, policyDraftError } from "./proxy-policy.ts";
import { warpEffectiveIP } from "./warp-health.ts";

const proxy = (extra = {}) => ({ id: 2, name: "合成出口", kind: "http", enabled: true, status: "healthy", egress_ip: "203.0.113.2", ...extra });
const policy = (extra = {}) => ({ subject_kind: "task", subject_id: "1", proxy_mode: "group", proxy_id: "2", proxy_revision: 4, allowed_group_ids: ["3"], agent_failover_enabled: false, failover_cooldown_seconds: 60, failover_max_switches: 3, authorization_revision: 1, proxy: proxy(), exit_verified_at: "2026-10-14T10:00:00Z", latest_switch: null, cooldown_until: null, remaining_switches: 3, ...extra });

test("策略载荷为 CAS 和 ID，不复制 URL/凭据；离开分组显式清授权", () => {
  const draft = draftFromPolicy(policy());
  const input = policyInput(draft, 4);
  assert.equal(input.expected_revision, 4);
  assert.deepEqual(input.allowed_group_ids, ["3"]);
  assert.equal(input.agent_failover_enabled, false);
  assert.equal(input.proxy_id, null);
  assert.equal(JSON.stringify(input).includes("203.0.113"), false);
  assert.deepEqual(policyInput({ ...draft, mode: "direct", groupIDs: [] }, 4).allowed_group_ids, []);
  assert.throws(() => policyInput({ ...draft, groupIDs: [] }, 4), /至少/);
  assert.throws(() => policyInput({ ...draft, groupIDs: ["3", "3"] }, 4), /重复/);
  assert.throws(() => policyInput({ ...draft, budget: "1001" }, 4), /预算/);
  assert.throws(() => policyInput(draft, undefined), /版本/);
});

test("同事件循环双击仅一次写入，成功须回读并匹配草稿", async () => {
  const flow = new RevisionSubmission();
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  let writes = 0, reads = 0;
  const run = () => flow.submit(async () => { writes++; await gate; }, async () => { reads++; return policy({ proxy_revision: 5 }); }, (value) => policyMatches(value, policyInput(draftFromPolicy(policy()), 4)));
  const first = run();
  assert.equal(await run(), null);
  assert.equal(flow.confirmed, false);
  release(); await first;
  assert.equal(writes, 1); assert.equal(reads, 1); assert.equal(flow.confirmed, true);
});

for (const status of [409, 500, undefined]) test(`冲突/未知 ${status}：只 GET 核对，保留草稿且明确确认后才新写入`, async () => {
  const flow = new RevisionSubmission(); let writes = 0, reads = 0;
  const draft = draftFromPolicy(policy());
  const write = async () => { writes++; throw Object.assign(new Error("合成失败"), { status }); };
  const read = async () => { reads++; return policy({ proxy_revision: 7 }); };
  await assert.rejects(flow.submit(write, read, () => false));
  assert.equal(flow.needsReadback, true); assert.equal(flow.confirmNewIntent(), false);
  await flow.submit(write, read, () => false);
  assert.equal(writes, 1); assert.equal(reads, 1); assert.equal(flow.confirmed, false);
  assert.deepEqual(draft.groupIDs, ["3"]);
  await assert.rejects(flow.submit(write, read, () => false), /确认/);
  assert.equal(flow.confirmNewIntent(), true);
});

test("PUT已受理但GET断网，仅GET核对，不能重复采样inherit", async () => {
  const flow = new RevisionSubmission(); let writes = 0;
  await assert.rejects(flow.submit(async () => { writes++; }, async () => { throw new Error("断网"); }, () => true));
  assert.equal(flow.needsReadback, true);
  await flow.submit(async () => { writes++; }, async () => policy(), () => true);
  assert.equal(writes, 1); assert.equal(flow.confirmed, true);
});

test("202始终pending，禁止重复意图与乐观完成", async () => {
  const flow = new RevisionSubmission(); const request = { id: "9", state: "pending" };
  await flow.submit(async () => ({ pending: request }), async () => policy({ latest_switch: request }), () => true);
  assert.equal(flow.confirmed, false); assert.equal(flow.pending.id, "9");
  assert.equal(flow.confirmNewIntent(), false);
});

test("Resin组/对话禁选；组外候选不进入获准并集，耗尽不直连", () => {
  const resin = proxy({ kind: "resin" });
  assert.match(proxyUnavailable(resin, "group"), /group_resin/);
  assert.match(proxyUnavailable(resin, "fixed-conversation"), /conversation_resin/);
  assert.equal(proxyUnavailable(resin, "fixed-task"), "");
  assert.match(groupUnavailable({ enabled: true, members: [{ proxy: resin }] }), /不会回落直连/);
  const groups = [{ id: "3", enabled: true, members: [{ proxy_id: "2", proxy: proxy() }, { proxy_id: "4", proxy: resin }] }, { id: "4", enabled: true, members: [{ proxy_id: "99", proxy: proxy({ id: 99 }) }] }];
  assert.deepEqual(allowedCandidates(policy(), groups).map((item) => item.proxy_id), ["2"]);
  assert.deepEqual(allowedCandidates(policy({ allowed_group_ids: [] }), groups), []);
  assert.deepEqual(allowedCandidates(policy(), [{ ...groups[0], enabled: false }]), []);
  assert.deepEqual(allowedCandidates(policy({ proxy_mode: "direct" }), groups), []);
});

test("有效IP隐藏未知/失败/离线/处理中/人工恢复，历史IP不补回", () => {
  assert.equal(effectiveExit(policy()), "203.0.113.2");
  assert.equal(effectiveExit(policy(), true), null);
  assert.equal(effectiveExit(policy({ exit_verified_at: null })), null);
  assert.equal(effectiveExit(policy({ proxy: proxy({ status: "failed" }) })), null);
  for (const state of ["pending", "draining", "applying", "failed", "recovery_required"]) assert.equal(effectiveExit(policy({ latest_switch: { state } })), null);
  const warp = proxy({ kind: "warp", warp_phase: "ready", warp_health: { proxy_id: 2, desired_running: true, state: "ready", last_checked_at: "2026-10-14T10:00:00Z", egress_ip: "203.0.113.4" } });
  assert.equal(warpEffectiveIP(warp), "203.0.113.4");
  for (const state of ["unknown", "failed", "deferred", "repairing", "checking", "pending", "disabled", "stopped"]) assert.equal(warpEffectiveIP({ ...warp, warp_health: { ...warp.warp_health, state } }), null);
  assert.equal(warpEffectiveIP({ ...warp, warp_health: undefined }), null);
  assert.equal(warpEffectiveIP({ ...warp, enabled: false }), null);
  assert.equal(warpEffectiveIP({ ...warp, warp_health: { ...warp.warp_health, desired_running: false } }), null);
});

test("冷却使用服务端时间，非法时间不产生虚构倒计时", () => {
  assert.equal(cooldownRemaining("2026-10-14T10:00:30Z", Date.parse("2026-10-14T10:00:00Z")), 30);
  assert.equal(cooldownRemaining("错误", Date.now()), 0);
});

test("持久恢复屏障独立于最近请求状态；隐藏IP，拒绝完成误判", () => {
  const value = policy({ recovery_required: true });
  assert.equal(policyRecoveryRequired(value), true);
  assert.equal(effectiveExit(value), null);
  assert.equal(policyRecoveryMatches(value, { expected_revision: 4, action: "retry" }), false);
  assert.equal(policyRecoveryMatches(policy({ latest_switch: { state: "pending" } }), { expected_revision: 4, action: "retry" }), false);
});
test("主体WARP有效IP使用正式健康DTO，不依赖旧顶层字段", () => {
  const warp = proxy({ kind: "warp", egress_ip: undefined, warp_health: { proxy_id: 2, desired_running: true, state: "ready", last_checked_at: "2026-10-14T10:00:00Z", egress_ip: "203.0.113.4" } });
  assert.equal(effectiveExit(policy({ proxy: warp })), "203.0.113.4");
  assert.equal(effectiveExit(policy({ proxy: { ...warp, egress_ip: "203.0.113.9", warp_health: { ...warp.warp_health, state: "failed" } } })), null);
});
test("原目标恢复不发送新模式、不采样inherit、不切直连、不发送退出证明", () => {
  const draft = { ...draftFromPolicy(policy()), mode: "direct" };
  assert.deepEqual(policyRecoveryInput("retry", 4, draft), { expected_revision: 4, action: "retry" });
  assert.deepEqual(policyRecoveryInput("confirm_released", 4, draft, true), { expected_revision: 4, action: "confirm_released" });
  assert.throws(() => policyRecoveryInput("abort_direct", 4, draft), /正式/);
  assert.throws(() => policyRecoveryInput("confirm_released", 4, draft), /人工核对/);
});
test("更换恢复策略须完整CAS、明确组授权；禁选Resin和组外固定无效项", () => {
  const draft = draftFromPolicy(policy());
  assert.deepEqual(policyRecoveryInput("replace_policy", 4, draft), { ...policyInput(draft, 4), action: "replace_policy" });
  assert.deepEqual(policyRecoveryInput("confirm_released_replace_policy", 4, draft, true), { ...policyInput(draft, 4), action: "confirm_released_replace_policy" });
  const next = policy({ proxy_revision: 5, recovery_required: false });
  assert.equal(policyRecoveryMatches(next, policyRecoveryInput("replace_policy", 4, draft)), true);
  assert.equal(policyRecoveryMatches({ ...next, proxy_mode: "direct" }, policyRecoveryInput("replace_policy", 4, draft)), false);
  assert.match(policyDraftError({ ...draft, mode: "fixed", proxyID: "99" }, 4, { kind: "task", id: "1" }, [], []), /缺失/);
  assert.match(policyDraftError({ ...draft, mode: "fixed", proxyID: "2" }, 4, { kind: "conversation", id: "1" }, [], [proxy({ kind: "resin" })]), /conversation_resin/);
});
