// 取消和迟到回读使用路由门控，只验证真实 UI 的编辑会话隔离。
async (page) => {
  const base = "http://127.0.0.1:19431";
  const check = (value, message) => { if (!value) throw new Error(message); };
  page.setDefaultTimeout(8000);
  await page.goto(`${base}/function/tasks/detail/?id=12`);
  const state = (await (await page.request.get(`${base}/__fixture/evidence`)).json()).policy;
  const clean = { ...state, latest_switch: null, proxy_mode: "group", recovery_required: false };
  await page.request.post(`${base}/__fixture/control`, { data: { behavior: {}, policy: clean } });
  let release, resolveCaptured, resolveFinished;
  const gate = new Promise((resolve) => { release = resolve; });
  const captured = new Promise((resolve) => { resolveCaptured = resolve; });
  const finished = new Promise((resolve) => { resolveFinished = resolve; });
  let count = 0;
  await page.route("**/api/tasks/12/proxy", async (route) => {
    if (route.request().method() !== "GET") return route.continue();
    if (++count === 1) {
      resolveCaptured(); await gate;
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ...clean, proxy_mode: "fixed", proxy_id: "3", proxy_revision: 0 }) });
      resolveFinished();
    } else await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(clean) });
  });
  const trigger = page.getByRole("button", { name: "切换任务代理", exact: true });
  await trigger.click(); await captured;
  let dialog = page.getByRole("dialog", { name: "任务出口策略" });
  await dialog.getByRole("button", { name: "关闭", exact: true }).click();
  await trigger.click();
  dialog = page.getByRole("dialog", { name: "任务出口策略" });
  const mode = dialog.getByLabel("连接方式", { exact: true });
  await mode.waitFor();
  check(await mode.inputValue() === "group", "新会话没有当前策略");
  release(); await finished;
  await mode.selectOption("direct");
  check(await mode.inputValue() === "direct", "旧GET污染新选择");
  await page.screenshot({ path: "output/playwright/proxy-groups/cancel-late-read.png", fullPage: true });
  const before = (await (await page.request.get(`${base}/__fixture/evidence`)).json()).records.filter((record) => record.method === "PUT" && record.path === "/api/tasks/12/proxy").length;
  await dialog.getByRole("button", { name: "关闭", exact: true }).click();
  const after = (await (await page.request.get(`${base}/__fixture/evidence`)).json()).records.filter((record) => record.method === "PUT" && record.path === "/api/tasks/12/proxy").length;
  check(after === before, "取消提交了策略PUT");
  await page.unroute("**/api/tasks/12/proxy");
  return { synthetic: true, passed: true, cancelWrites: 0, lateReadIgnored: true };
}
