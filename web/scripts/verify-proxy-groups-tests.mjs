// 整组行为测试最多 60 秒。超时终止本次子进程，不影响其他验证服务。
import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const output = resolve(root, "output/playwright/proxy-groups");
mkdirSync(output, { recursive: true });
const tests = ["proxy-policy", "proxy-policy-api", "warp-health-api", "task-proxy", "task-proxy-api", "warp-create-intent", "warp-create-api", "proxy-center", "proxy-warp-register"];
const started = Date.now();
const child = spawn(process.execPath, ["--import", "./scripts/test-api-loader.mjs", "--test", "--test-timeout=60000", ...tests.map((name) => `src/lib/${name}.test.mjs`)], { cwd: root, windowsHide: true });
let log = "", timedOut = false;
child.stdout.on("data", (chunk) => { log += chunk; });
child.stderr.on("data", (chunk) => { log += chunk; });
const timer = setTimeout(() => { timedOut = true; child.kill(); }, 60000);
child.on("error", (error) => { clearTimeout(timer); writeFileSync(resolve(output, "unit-tests.log"), `${log}\n启动失败：${error.message}\n`); process.exitCode = 1; });
child.on("close", (code) => {
  clearTimeout(timer);
  const result = { exit: code, timedOut, timeoutMs: 60000, durationMs: Date.now() - started };
  writeFileSync(resolve(output, "unit-tests.log"), `${log}\n${JSON.stringify(result)}\n`);
  console.log(JSON.stringify(result));
  process.exitCode = timedOut ? 1 : code ?? 1;
});
