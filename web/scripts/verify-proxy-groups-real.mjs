import { mkdirSync } from "node:fs";
import { createRequire } from "node:module";

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`缺少 ${name}`);
  return value;
};

const playwrightModule = required("ARTEX_TEST_PLAYWRIGHT_MODULE");
const chromiumExecutable = required("ARTEX_TEST_CHROMIUM");
const token = required("ARTEX_REAL_UI_TOKEN");
const taskID = required("ARTEX_REAL_UI_TASK_ID");
const proxyAID = required("ARTEX_REAL_UI_PROXY_A_ID");
const proxyBID = required("ARTEX_REAL_UI_PROXY_B_ID");
const base = process.env.ARTEX_REAL_UI_BASE?.trim() || "http://127.0.0.1:19432";
const groupName = `UI真实GoPG组-${Date.now()}`;
const { chromium } = createRequire(import.meta.url)(playwrightModule);

const check = (condition, message) => {
  if (!condition) throw new Error(message);
};

mkdirSync("output/playwright/proxy-groups-real", { recursive: true });
const browser = await chromium.launch({
  executablePath: chromiumExecutable,
  headless: true,
  args: ["--disable-background-networking", "--disable-component-update"],
});

try {
  const context = await browser.newContext({
    extraHTTPHeaders: { Authorization: `Bearer ${token}` },
  });
  await context.addCookies([{ name: "artex_token", value: token, url: base }]);
  await context.addInitScript((value) => localStorage.setItem("artex_token", value), token);
  const page = await context.newPage();
  page.setDefaultTimeout(10_000);

  await page.goto(`${base}/system/proxies/`, { waitUntil: "domcontentloaded" });
  try {
    await page.getByText("http://127.0.0.1:19111/", { exact: false }).first().waitFor();
  } catch {
    throw new Error(`代理中心未加载真实目录：${await page.locator("body").innerText()}`);
  }
  await page.getByRole("button", { name: "新建分组", exact: true }).click();
  let dialog = page.getByRole("dialog");
  await dialog.getByRole("textbox", { name: "分组名称" }).fill(groupName);
  await dialog.getByRole("button", { name: "创建分组", exact: true }).click();
  await dialog.getByText("分组已创建。", { exact: false }).waitFor();
  const search = dialog.getByRole("textbox", { name: "搜索现有代理" });
  await search.fill(proxyAID);
  check((await dialog.innerText()).includes(`#${proxyAID}`), `代理 A 未进入编辑器：${await dialog.innerText()}`);
  await dialog.getByText(`#${proxyAID}`, { exact: false }).click();
  await search.fill(proxyBID);
  await dialog.getByText(`#${proxyBID}`, { exact: false }).click();
  await dialog.getByRole("button", { name: "保存成员编号", exact: true }).click();
  await dialog.getByText("成员已保存并回读确认", { exact: false }).waitFor();

  let response = await context.request.get(`${base}/api/proxy-groups`);
  check(response.ok(), `分组回读失败 ${response.status()}`);
  const groups = (await response.json()).groups;
  const group = groups.find((value) => value.name === groupName);
  check(group, "真实 UI 创建的分组未进入 Go/PG");
  check(
    JSON.stringify(group.members.map((member) => member.proxy_id)) === JSON.stringify([proxyAID, proxyBID]),
    `成员保存不一致 ${JSON.stringify(group.members)}`,
  );
  await dialog.getByRole("button", { name: "关闭", exact: true }).click();

  await page.goto(`${base}/function/tasks/detail/?id=${taskID}`, { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: "切换任务代理", exact: true }).click();
  dialog = page.getByRole("dialog", { name: "任务出口策略" });
  const mode = dialog.getByLabel("连接方式", { exact: true });
  await mode.selectOption("group");
  await dialog.getByRole("checkbox", { name: new RegExp(groupName) }).check();
  await dialog.getByRole("switch", { name: "允许 Agent 在获准组内故障切换" }).check();
  await dialog.getByRole("button", { name: "保存出口策略", exact: true }).click();
  await dialog.getByText("保存已确认", { exact: false }).waitFor();

  response = await context.request.get(`${base}/api/tasks/${taskID}/proxy`);
  check(response.ok(), `group 策略回读失败 ${response.status()}`);
  const grouped = await response.json();
  check(grouped.proxy_mode === "group", `未保存 group：${JSON.stringify(grouped)}`);
  check(grouped.allowed_group_ids.includes(group.id), "真实策略缺少已授权组");
  check(grouped.agent_failover_enabled === true, "真实策略未保存 Agent 显式授权");
  check([proxyAID, proxyBID].includes(grouped.proxy_id), "group 初选未使用组内成员");

  await mode.selectOption("direct");
  await dialog.getByRole("button", { name: "保存出口策略", exact: true }).click();
  await dialog.getByText("保存已确认", { exact: false }).waitFor();
  response = await context.request.get(`${base}/api/tasks/${taskID}/proxy`);
  check(response.ok(), `direct 策略回读失败 ${response.status()}`);
  const direct = await response.json();
  check(direct.proxy_mode === "direct" && direct.proxy_id === null, `未切换 direct：${JSON.stringify(direct)}`);
  await page.screenshot({ path: "output/playwright/proxy-groups-real/ui-go-pg-direct.png", fullPage: true });

  process.stdout.write(
    JSON.stringify({
      passed: true,
      layer: "Chromium -> Next UI -> Go API -> isolated PostgreSQL",
      group_id: group.id,
      member_ids: group.members.map((member) => member.proxy_id),
      group_revision: grouped.proxy_revision,
      direct_revision: direct.proxy_revision,
      final_mode: direct.proxy_mode,
    }),
  );
} finally {
  await browser.close();
}
