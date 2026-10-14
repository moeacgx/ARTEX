import { APIError, api } from "./api.ts";
import { TaskProxySubmission, taskProxyInput } from "./task-proxy.ts";
import assert from "node:assert/strict";
import test from "node:test";

const originalFetch = globalThis.fetch;
test.afterEach(() => {
  globalThis.fetch = originalFetch;
});

for (const mode of ["fixed", "direct", "inherit"]) {
  test(`API客户端载荷 ${mode}：PUT仅发送选择，随后GET回读当前Task`, async () => {
    const calls = [];
    const expected = { proxy_mode: mode, proxy_id: mode === "fixed" ? "4" : null, expected_revision: 0 };
    const current = {
      id: "12",
      status: "paused",
      proxy_revision: 1,
      proxy_mode: mode,
      proxy_id: mode === "direct" ? undefined : 4,
    };
    globalThis.fetch = async (url, init) => {
      if (init?.method !== "PUT") assert.equal(init.cache, "no-store");
      calls.push({ url, method: init?.method ?? "GET", body: init?.body ? JSON.parse(init.body) : null });
      return Response.json(init?.method === "PUT" ? { ...current, proxy_id: 99 } : current);
    };
    const flow = new TaskProxySubmission();
    const result = await flow.save("12", taskProxyInput({ mode, id: "4" }, 0), {
      write: api.updateTaskProxy,
      read: api.task,
    });
    assert.deepEqual(calls, [
      { url: "/api/tasks/12/proxy", method: "PUT", body: expected },
      { url: "/api/tasks/12", method: "GET", body: null },
    ]);
    assert.deepEqual(result, JSON.parse(JSON.stringify(current)));
  });
}

for (const status of [400, 404, 409, 500]) {
  test(`API客户端错误 ${status}：保留状态及脱敏说明，无自动重试`, async () => {
    let count = 0;
    const payload = { error: status === 409 ? "仍有工具在途，请等待退出" : "合成错误" };
    globalThis.fetch = async () => {
      count++;
      return Response.json(payload, { status });
    };
    await assert.rejects(
      api.updateTaskProxy("12", { proxy_mode: "direct", proxy_id: null, expected_revision: 0 }),
      (error) => {
        assert.ok(error instanceof APIError);
        assert.equal(error.status, status);
        assert.deepEqual(error.payload, payload);
        return true;
      },
    );
    assert.equal(count, 1);
  });
}

test("API客户端CAS：缺失/非法版本零请求，409冲突完整保留code/current_revision", async () => {
  let calls = 0;
  const payload = { error: "版本已更新", code: "proxy_revision_conflict", current_revision: 8 };
  globalThis.fetch = async () => {
    calls++;
    return Response.json(payload, { status: 409 });
  };
  for (const expected_revision of [undefined, null, -1, 1.2, "0", 9007199254740992])
    assert.throws(() => api.updateTaskProxy("12", { proxy_mode: "inherit", proxy_id: null, expected_revision }));
  assert.equal(calls, 0);
  await assert.rejects(
    api.updateTaskProxy("12", { proxy_mode: "inherit", proxy_id: null, expected_revision: 0 }),
    (error) => {
      assert.deepEqual(error.payload, payload);
      return true;
    },
  );
  assert.equal(calls, 1);
});
