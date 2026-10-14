import {
  TaskProxySubmission,
  taskProxyDraft,
  taskProxyInput,
  taskProxyOptionLabel,
  taskProxySwitchError,
  taskProxySwitchSelectable,
  validProxyRevision,
} from "./task-proxy.ts";
import assert from "node:assert/strict";
import test from "node:test";

const item = (extra = {}) => ({ id: "4", name: "出口四", kind: "http", enabled: true, status: "healthy", ...extra });
const task = (extra = {}) => ({
  id: "12",
  status: "paused",
  proxy_mode: "inherit",
  proxy_revision: 0,
  proxy_id: 2,
  proxy: { id: "2", name: "旧快照", kind: "http" },
  ...extra,
});
const selection = (extra = {}) => ({
  task: task(),
  draft: { mode: "fixed", id: "4" },
  items: [item()],
  globalID: "4",
  loading: false,
  error: "",
  ...extra,
});

test("回填保留原 inherit 快照，不将它改为 fixed 或当前默认；未知模式不猜直连", () => {
  assert.deepEqual(taskProxyDraft(task()), { mode: "inherit", id: "2" });
  assert.deepEqual(taskProxyDraft(task({ proxy_mode: "direct", proxy_id: undefined, proxy: undefined })), {
    mode: "direct",
    id: "",
  });
  assert.deepEqual(taskProxyDraft(task({ proxy_mode: "fixed" })), { mode: "fixed", id: "2" });
  assert.equal(taskProxyDraft(task({ proxy_mode: undefined })).mode, "");
});

test("切换三模式只发模式/字符串ID；数字历史载荷仍由服务端兼容", () => {
  assert.deepEqual(taskProxyInput({ mode: "inherit", id: "2" }, 0), {
    proxy_mode: "inherit",
    proxy_id: null,
    expected_revision: 0,
  });
  assert.deepEqual(taskProxyInput({ mode: "direct", id: "2" }, 0), {
    proxy_mode: "direct",
    proxy_id: null,
    expected_revision: 0,
  });
  assert.deepEqual(taskProxyInput({ mode: "fixed", id: "4" }, 0), {
    proxy_mode: "fixed",
    proxy_id: "4",
    expected_revision: 0,
  });
  assert.equal(taskProxyInput({ mode: "fixed", id: "9007199254740993" }, 0).proxy_id, "9007199254740993");
  for (const id of ["", "0", "-1", "1e3", "1.5"])
    assert.throws(() => taskProxyInput({ mode: "fixed", id }, 0));
  assert.throws(() => taskProxyInput({ mode: "", id: "" }, 0));
});

test("unknown/禁用/失败/旧WARP IP均闭锁，正常任务Resin仍支持", () => {
  for (const patch of [
    { status: "unknown" },
    { enabled: false },
    { status: "failed" },
    { status: "stopped" },
    { kind: "warp", warp_phase: "failed", egress_ip: "198.51.100.99" },
  ]) {
    const bad = item(patch);
    assert.equal(taskProxySwitchSelectable(bad), false);
    assert.match(taskProxySwitchError(selection({ items: [bad] })), /不可用/);
    assert.equal(taskProxyOptionLabel(bad).includes("198.51.100.99"), false);
  }
  assert.equal(taskProxySwitchSelectable(item({ kind: "resin" })), true);
  assert.equal(taskProxySwitchSelectable(item({ kind: "warp", warp_phase: "ready", egress_ip: "203.0.113.4" })), true);
});

test("目录失败阻止三模式，inherit验证当前default而不是旧快照；保留原失效ID", () => {
  for (const mode of ["inherit", "direct", "fixed"]) {
    assert.match(taskProxySwitchError(selection({ draft: { mode, id: "4" }, loading: true })), /加载/);
    assert.match(taskProxySwitchError(selection({ draft: { mode, id: "4" }, error: "合成错误" })), /目录/);
  }
  assert.equal(taskProxySwitchError(selection({ draft: { mode: "inherit", id: "2" } })), "");
  assert.match(taskProxySwitchError(selection({ draft: { mode: "inherit", id: "2" }, globalID: 99 })), /#99/);
  assert.equal(taskProxySwitchError(selection({ draft: { mode: "inherit", id: "2" }, globalID: null })), "");
  assert.match(taskProxySwitchError(selection({ draft: { mode: "fixed", id: "2" } })), /#2/);
});

test("运行/排队和已知在途提示先暂停等待，终态仍由后端409保护", () => {
  for (const patch of [{ status: "running" }, { status: "queued" }, { queued: true }, { in_flight: 1 }])
    assert.match(taskProxySwitchError(selection({ task: task(patch) })), /暂停|退出/);
  assert.equal(taskProxySwitchError(selection({ task: task({ status: "done" }) })), "");
});

test("同步双击只PUT一次；成功使用GET回读值，不能用PUT回包/草稿更新徽标", async () => {
  let writes = 0;
  let reads = 0;
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const flow = new TaskProxySubmission();
  const input = taskProxyInput({ mode: "fixed", id: "4" }, 0);
  const client = {
    write: async () => {
      writes++;
      await gate;
      return task({ proxy_mode: "fixed", proxy_id: 4, proxy_revision: 1 });
    },
    read: async () => {
      reads++;
      return task({ proxy_mode: "direct", proxy_id: undefined, proxy: undefined });
    },
  };
  const first = flow.save("12", input, client);
  assert.equal(await flow.save("12", input, client), null);
  release();
  assert.equal((await first).proxy_mode, "direct");
  assert.equal(writes, 1);
  assert.equal(reads, 1);
});

test("PUT成功GET失败进入待回读，重试只GET，不重复inherit取快照", async () => {
  let writes = 0;
  let reads = 0;
  const flow = new TaskProxySubmission();
  const input = taskProxyInput({ mode: "inherit", id: "2" }, 0);
  const client = {
    write: async () => {
      writes++;
    },
    read: async () => {
      if (++reads === 1) throw new Error("合成回读失败");
      return task({ proxy_id: 4, proxy_revision: 1 });
    },
  };
  await assert.rejects(flow.save("12", input, client), /回读失败/);
  assert.equal(flow.needsReadback, true);
  assert.equal((await flow.save("12", input, client)).proxy_id, 4);
  assert.equal(writes, 1);
  assert.equal(flow.needsReadback, false);
});

test("409不重试不GET；500/网络未知只允许核对GET，选择由调用方保留", async () => {
  for (const status of [409, 500, undefined]) {
    const flow = new TaskProxySubmission();
    let writes = 0;
    let reads = 0;
    const input = taskProxyInput({ mode: "fixed", id: "4" }, 0);
    const client = {
      write: async () => {
        writes++;
        throw Object.assign(new Error("合成保存失败"), { status });
      },
      read: async () => {
        reads++;
        return task();
      },
    };
    await assert.rejects(flow.save("12", input, client));
    assert.equal(writes, 1);
    assert.equal(reads, 0);
    assert.equal(flow.needsReadback, status !== 409);
    assert.deepEqual(input, { proxy_mode: "fixed", proxy_id: "4", expected_revision: 0 });
  }
});

test("CAS版本必填且非负安全整数，缺失不能当成旧任务0", () => {
  for (const revision of [undefined, null, -1, 1.5, "0", NaN, Infinity, 9007199254740992]) {
    assert.equal(validProxyRevision(revision), false);
    assert.throws(() => taskProxyInput({ mode: "inherit", id: "" }, revision), /版本/);
    assert.match(taskProxySwitchError(selection({ task: task({ proxy_revision: revision }) })), /版本/);
  }
  assert.equal(taskProxyInput({ mode: "inherit", id: "" }, 0).expected_revision, 0);
});

test("CAS冲突只GET核对；旧revision不自动更新重PUT，明确新意图才可再次保存", async () => {
  const flow = new TaskProxySubmission();
  const revisions = [];
  let reads = 0;
  const client = {
    write: async (_, input) => {
      revisions.push(input.expected_revision);
      if (revisions.length === 1)
        throw Object.assign(new Error("版本冲突"), {
          status: 409,
          payload: { code: "proxy_revision_conflict", current_revision: 7 },
        });
    },
    read: async () => {
      reads++;
      return task({ proxy_revision: 7, proxy_id: 4 });
    },
  };
  const old = taskProxyInput({ mode: "inherit", id: "2" }, 0);
  await assert.rejects(flow.save("12", old, client));
  assert.equal(flow.confirmNewIntent(), false);
  assert.equal((await flow.save("12", old, client)).proxy_revision, 7);
  assert.equal(flow.awaitingConfirmation, true);
  await assert.rejects(flow.save("12", old, client), /新.*选择/);
  assert.deepEqual(revisions, [0]);
  assert.equal(reads, 1);
  assert.equal(flow.confirmNewIntent(), true);
  await flow.save("12", taskProxyInput({ mode: "direct", id: "" }, 7), client);
  assert.deepEqual(revisions, [0, 7]);
});

test("未知结果GET回读后仍禁止自动PUT；回读缺版本不能完成", async () => {
  const flow = new TaskProxySubmission();
  let writes = 0;
  let reads = 0;
  const client = {
    write: async () => {
      writes++;
      throw new TypeError("网络断开");
    },
    read: async () => {
      reads++;
      return task({ proxy_revision: reads === 1 ? undefined : 2 });
    },
  };
  const input = taskProxyInput({ mode: "inherit", id: "2" }, 0);
  await assert.rejects(flow.save("12", input, client));
  await assert.rejects(flow.save("12", input, client), /版本/);
  assert.equal(flow.needsReadback, true);
  await flow.save("12", input, client);
  await assert.rejects(flow.save("12", input, client), /新.*选择/);
  assert.equal(writes, 1);
  assert.equal(input.expected_revision, 0);
});
