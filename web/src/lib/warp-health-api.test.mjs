import assert from "node:assert/strict";
import test from "node:test";
import { warpHealthAPI } from "./warp-health-api.ts";
import { RevisionSubmission } from "./proxy-policy.ts";

const original = globalThis.fetch;
const dto = { proxy_id: 5, revision: 2, auto_recover_enabled: true, probe_interval_seconds: 60, failure_threshold: 3, recovery_min_seconds: 300, recovery_max_seconds: 3600 };
test.afterEach(() => { globalThis.fetch = original; });
test("健康开关 GET 权威版本，PUT 仅 CAS 与布尔，不发送 desired/注册", async () => {
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, method: init?.method ?? "GET", body: init?.body ? JSON.parse(init.body) : null }); return Response.json(dto); };
  assert.deepEqual(await warpHealthAPI.policy(5), dto);
  await warpHealthAPI.save(5, 2, false);
  assert.deepEqual(calls, [{ url: "/api/proxies/5/health-policy", method: "GET", body: null }, { url: "/api/proxies/5/health-policy", method: "PUT", body: { expected_revision: 2, auto_recover_enabled: false } }]);
});
test("健康策略编号/版本无效阻止保存；未知开关不能猜默认true", async () => {
  globalThis.fetch = async () => Response.json({ ...dto, proxy_id: 6 });
  await assert.rejects(warpHealthAPI.policy(5), /编号/);
  assert.throws(() => warpHealthAPI.save(5, undefined, true), /版本/);
  assert.throws(() => warpHealthAPI.save(5, 0, undefined), /开关/);
});
for (const status of [409, 500]) test(`健康开关 ${status}：保留草稿，仅GET回读，不重放PUT`, async () => {
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push(init?.method ?? "GET"); return init?.method === "PUT" ? Response.json({ code: "warp_health_revision_conflict", error: "合成失败", revision: 3 }, { status }) : Response.json({ ...dto, revision: 3 }); };
  const flow = new RevisionSubmission();
  await assert.rejects(flow.submit(async () => { await warpHealthAPI.save(5, 2, false); }, () => warpHealthAPI.policy(5), () => true));
  await flow.submit(async () => { await warpHealthAPI.save(5, 2, false); }, () => warpHealthAPI.policy(5), () => true);
  assert.deepEqual(calls, ["PUT", "GET"]); assert.equal(flow.confirmed, false);
});
test("人工恢复仅发空对象，拒绝前端自证；409/断网不自动重试", async () => {
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, method: init.method, body: JSON.parse(init.body) }); return Response.json({ error: "真实进程未退出", code: "warp_health_recovery_required" }, { status: 409 }); };
  await assert.rejects(warpHealthAPI.recover(5), (error) => error.status === 409 && error.payload.code === "warp_health_recovery_required");
  assert.deepEqual(calls, [{ url: "/api/proxies/5/health/recovery", method: "POST", body: {} }]);
});
