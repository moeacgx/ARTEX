import assert from "node:assert/strict";
import test from "node:test";
import { proxyPolicyAPI } from "./proxy-policy-api.ts";
import { RevisionSubmission } from "./proxy-policy.ts";

const original = globalThis.fetch;
test.afterEach(() => { globalThis.fetch = original; });
test("组成员完整替换/CAS与删除query精确接线，不发凭据", async () => {
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, method: init?.method ?? "GET", body: init?.body ? JSON.parse(init.body) : null }); return init?.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({ id: "7" }); };
  await proxyPolicyAPI.members("7", 4, ["1", "2"]);
  await proxyPolicyAPI.deleteGroup("7", 5);
  assert.deepEqual(calls, [{ url: "/api/proxy-groups/7/members", method: "PUT", body: { expected_revision: 4, proxy_ids: ["1", "2"] } }, { url: "/api/proxy-groups/7?expected_revision=5", method: "DELETE", body: null }]);
});
test("任务/对话路径及DTO主体隔离，GET不重新采样", async () => {
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push(url); assert.equal(init.cache, "no-store"); return Response.json({ subject_kind: "conversation", subject_id: "2", proxy_revision: 5, proxy_mode: "direct", allowed_group_ids: [], agent_failover_enabled: false, failover_cooldown_seconds: 60, failover_max_switches: 3, remaining_switches: 3, recovery_required: false }); };
  await proxyPolicyAPI.policy({ kind: "conversation", id: "2" });
  await assert.rejects(proxyPolicyAPI.policy({ kind: "task", id: "2" }), /主体/);
  assert.deepEqual(calls, ["/api/conversations/2/proxy", "/api/tasks/2/proxy"]);
});
test("异步请求202返回pending原编号，成功写入202也不能当完成", async () => {
  let calls = 0;
  globalThis.fetch = async () => { calls++; return Response.json({ accepted: true, request: { id: "11", subject_kind: "task", subject_id: "2", state: "pending" }, end_turn: true }, { status: 202 }); };
  const subject = { kind: "task", id: "2" };
  const request = await proxyPolicyAPI.request(subject, { expected_revision: 4, reason: "连接故障", evidence: { kind: "connection_failure", reference: "运行时证据" } });
  assert.equal(request.state, "pending"); assert.equal(request.id, "11");
  await assert.rejects(proxyPolicyAPI.save(subject, { expected_revision: 4, proxy_mode: "direct" }), (error) => error.status === 202);
  assert.equal(calls, 2);
});
test("请求不能接受其他主体或非pending的202，绝不重试", async () => {
  let calls = 0;
  globalThis.fetch = async () => { calls++; return Response.json({ request: { id: "11", subject_kind: "conversation", subject_id: "99", state: "succeeded" } }, { status: 202 }); };
  await assert.rejects(proxyPolicyAPI.request({ kind: "task", id: "2" }, { expected_revision: 0 }));
  assert.equal(calls, 1);
});
test("任务/对话人工策略恢复POST使用专属路径与CAS，没有abort_direct或自证字段", async () => {
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, method: init.method, body: JSON.parse(init.body) }); return Response.json({}); };
  await proxyPolicyAPI.recover({ kind: "task", id: "2" }, { expected_revision: 5, action: "retry" });
  await proxyPolicyAPI.recover({ kind: "conversation", id: "3" }, { expected_revision: 8, action: "replace_policy", proxy_mode: "group", proxy_id: null, allowed_group_ids: ["1"], agent_failover_enabled: false, failover_cooldown_seconds: 60, failover_max_switches: 3 });
  assert.equal(calls[0].url, "/api/tasks/2/proxy/policy-recovery");
  assert.deepEqual(calls[0].body, { expected_revision: 5, action: "retry" });
  assert.equal(calls[1].url, "/api/conversations/3/proxy/policy-recovery");
  assert.equal(calls[1].body.agent_failover_enabled, false);
  assert.deepEqual(calls.map((call) => call.method), ["POST", "POST"]);
});
for (const status of [409, 500, undefined]) test(`人工策略恢复 ${status} 只GET核对，同步双击锁防重复POST`, async () => {
  const calls = [];
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  globalThis.fetch = async (url, init) => {
    calls.push(init.method ?? "GET");
    if (init.method === "POST") {
      await gate;
      if (!status) throw new Error("断网");
      return Response.json({ error: "恢复未确认" }, { status });
    }
    return Response.json({ subject_kind: "task", subject_id: "2", proxy_revision: 5, proxy_mode: "group", allowed_group_ids: ["1"], agent_failover_enabled: false, failover_cooldown_seconds: 60, failover_max_switches: 3, remaining_switches: 2, recovery_required: true });
  };
  const flow = new RevisionSubmission();
  const subject = { kind: "task", id: "2" };
  const run = () => flow.submit(() => proxyPolicyAPI.recover(subject, { expected_revision: 5, action: "retry" }), () => proxyPolicyAPI.policy(subject), () => false);
  const first = assert.rejects(run());
  assert.equal(await run(), null);
  release(); await first;
  assert.equal(flow.confirmNewIntent(), false);
  await flow.reconcile(() => proxyPolicyAPI.policy(subject));
  assert.deepEqual(calls, ["POST", "GET"]);
  assert.equal(flow.reviewRequired, true);
});
