import { APIError, api } from "./api.ts";
import { WarpCreateIntent } from "./warp-create-intent.ts";
import { warpCreateFailure } from "./warp-create-result.ts";
import assert from "node:assert/strict";
import test from "node:test";

const key = "11000000-0000-4000-8000-000000000001";
const body = { name: "合成 WARP", kind: "warp", accept_terms: true };
const proxy = { id: "31", ...body, enabled: true, status: "failed", warp_phase: "failed" };
const originalFetch = globalThis.fetch;
test.afterEach(() => {
  globalThis.fetch = originalFetch;
});

test("API 客户端载荷：首发和网络重试沿用已持久化的 key 与规范 payload", async () => {
  let stored;
  const storage = {
    getItem: () => stored ?? null,
    setItem: (_, value) => {
      stored = value;
    },
    removeItem: () => {
      stored = undefined;
    },
  };
  const flow = new WarpCreateIntent(
    () => storage,
    () => key,
  );
  flow.restore();
  const calls = [];
  globalThis.fetch = async (url, init) => {
    assert.deepEqual(JSON.parse(stored), { key, ...body });
    calls.push({ url, body: JSON.parse(init.body), key: init.headers["Idempotency-Key"] });
    if (calls.length === 1) throw new TypeError("合成断响应");
    return Response.json(
      { ...proxy, warp_phase: "ready", status: "healthy", egress_ip: "203.0.113.42" },
      { status: 201 },
    );
  };
  const first = flow.begin(" 合成 WARP ", true);
  await assert.rejects(api.createProxy(body, first.key));
  flow.finish("uncertain");
  await api.createProxy(body, flow.begin(body.name, true).key);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0], calls[1]);
});

for (const status of [202, 502]) {
  test(`API 客户端响应：${status} pending 保留包和原 ID，failed DTO 也不能进入 recover`, async () => {
    const payload = { error: "合成处理中", proxy_id: "31", proxy, operation_status: "pending" };
    globalThis.fetch = async () => Response.json(payload, { status });
    await assert.rejects(api.createProxy(body, key), (error) => {
      assert.ok(error instanceof APIError);
      assert.equal(error.status, status);
      assert.deepEqual(error.payload, payload);
      assert.deepEqual(warpCreateFailure(error), { stage: "pending", id: "31", proxy });
      return true;
    });
  });
}

test("API 客户端响应：502 failed、409冲突、410 tombstone 分别保留边界", async () => {
  for (const [status, stage] of [
    [502, "retained"],
    [409, "conflict"],
    [410, "deleted"],
  ]) {
    const payload = { error: "合成终态", operation_status: "failed", proxy_id: "31", proxy };
    globalThis.fetch = async () => Response.json(payload, { status });
    await assert.rejects(api.createProxy(body, key), (error) => {
      assert.deepEqual(error.payload, payload);
      assert.equal(warpCreateFailure(error).stage, stage);
      return true;
    });
  }
});

test("API 客户端：WARP缺/非法key请求前拒绝，普通代理不需要key", async () => {
  let requests = 0;
  globalThis.fetch = async (_, init) => {
    requests++;
    assert.equal(init.headers["Idempotency-Key"], undefined);
    return Response.json(proxy);
  };
  for (const invalid of [undefined, "", "new-key", "null"]) assert.throws(() => api.createProxy(body, invalid));
  assert.equal(requests, 0);
  await api.createProxy({ name: "HTTP", kind: "http", url: "http://127.0.0.1:8080" });
  assert.equal(requests, 1);
});

test("201重放当前非ready DTO只返回现状，客户端绝不另发注册", async () => {
  let requests = 0;
  globalThis.fetch = async (url) => {
    requests++;
    assert.equal(url, "/api/proxies");
    return Response.json(proxy, { status: 201 });
  };
  assert.deepEqual(await api.createProxy(body, key), proxy);
  assert.equal(requests, 1);
});
