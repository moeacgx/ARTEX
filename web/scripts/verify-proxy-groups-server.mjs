// 仅合成 HTTP 行为与前端截图。无 Go/PG、无公网注册，不占用旧验收端口。
import { createReadStream, existsSync, mkdirSync, writeFileSync } from "node:fs";
import http from "node:http";
import { extname, resolve, sep } from "node:path";

const root = resolve(import.meta.dirname, "..");
const output = resolve(root, "output/playwright/proxy-groups");
mkdirSync(output, { recursive: true });
const port = Number(process.env.PROXY_UI_FIXTURE_PORT ?? 19431);
const stamp = () => new Date().toISOString();
const proxies = [
  { id: 1, name: "合成 HTTP 一", kind: "http", enabled: true, status: "healthy", egress_ip: "203.0.113.1", last_checked_at: stamp() },
  { id: 2, name: "合成 HTTP 二", kind: "http", enabled: true, status: "healthy", egress_ip: "203.0.113.2", last_checked_at: stamp() },
  { id: 3, name: "组外出口", kind: "socks5", enabled: true, status: "healthy", egress_ip: "203.0.113.3" },
  { id: 4, name: "合成 Resin", kind: "resin", enabled: true, status: "healthy", egress_ip: "203.0.113.4" },
  { id: 5, name: "失败 WARP（旧 IP 不可用）", kind: "warp", enabled: true, status: "failed", warp_phase: "failed", egress_ip: "203.0.113.55", warp_health: { proxy_id: 5, desired_running: true, state: "failed", consecutive_failures: 3, recovery_attempts: 1, last_checked_at: stamp(), next_recovery_at: new Date(Date.now() + 300000).toISOString(), egress_ip: "", last_error: "合成连接故障" } },
];
let groups = [{ id: "1", name: "合成获准组", enabled: true, revision: 0, members: ["1", "2", "4"].map((id, position) => ({ proxy_id: id, position, proxy: proxies.find((proxy) => String(proxy.id) === id) })) }];
let policy = { subject_kind: "task", subject_id: "12", proxy_mode: "inherit", proxy_id: "1", proxy_revision: 0, allowed_group_ids: [], agent_failover_enabled: false, failover_cooldown_seconds: 60, failover_max_switches: 3, authorization_revision: 0, proxy: proxies[0], exit_verified_at: stamp(), latest_switch: null, cooldown_until: null, remaining_switches: 3, recovery_required: false };
let conversationPolicy = { ...policy, subject_kind: "conversation", subject_id: "8" };
const task = () => ({ id: "12", name: "合成代理分组任务", description: "前端合成 HTTP 验证，非 Go/PG", goal: "验证策略交互", status: "paused", paused: true, in_flight: 0, created_at: stamp(), ...policy });
let behavior = {};
let healthPolicy = { proxy_id: 5, revision: 0, auto_recover_enabled: true, probe_interval_seconds: 60, failure_threshold: 3, recovery_min_seconds: 300, recovery_max_seconds: 3600 };
const records = [];
const evidence = () => ({ synthetic: true, layer: "UI → 合成 Node HTTP；不是 Go/PG", groups, policy, conversationPolicy, healthPolicy, records });
const persist = () => writeFileSync(resolve(output, "synthetic-http-evidence.json"), JSON.stringify(evidence(), null, 2));
const json = (res, status, value) => { res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }); res.end(status === 204 ? undefined : JSON.stringify(value)); };
const body = async (req) => { const chunks = []; for await (const chunk of req) chunks.push(chunk); return chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {}; };
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://127.0.0.1:${port}`);
    if (url.pathname === "/__fixture/bootstrap") { res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" }); res.end("<script>localStorage.setItem('artex_token','synthetic-only');document.cookie='artex_token=synthetic-only; path=/';location.href='/system/proxies/'</script>"); return; }
    if (url.pathname === "/__fixture/evidence") return json(res, 200, evidence());
    if (url.pathname === "/__fixture/control") { const value = await body(req); behavior = value.behavior ?? {}; if (value.policy) policy = { ...policy, ...value.policy }; if (value.conversationPolicy) conversationPolicy = { ...conversationPolicy, ...value.conversationPolicy }; persist(); return json(res, 200, { ok: true }); }
    if (url.pathname === "/__fixture/stop") { json(res, 200, { ok: true }); server.close(); return; }
    if (url.pathname.startsWith("/api/")) {
      const input = ["POST", "PUT", "DELETE"].includes(req.method) ? await body(req) : null;
      const record = { method: req.method, path: url.pathname, body: input }; records.push(record);
      if (url.pathname === "/api/proxies") return json(res, 200, { proxies, global_proxy_id: 1 });
      if (url.pathname === "/api/proxies/5") return json(res, 200, proxies[4]);
      if (url.pathname === "/api/proxies/5/health-policy") {
        if (req.method === "GET") return json(res, 200, healthPolicy);
        if (behavior.healthConflict || input.expected_revision !== healthPolicy.revision) return json(res, 409, { code: "warp_health_revision_conflict", error: "合成健康策略版本冲突", revision: healthPolicy.revision });
        healthPolicy = { ...healthPolicy, revision: healthPolicy.revision + 1, auto_recover_enabled: input.auto_recover_enabled };
        persist();
        if (behavior.healthUnknown) return json(res, 500, { error: "合成健康策略提交后未知" });
        return json(res, 200, healthPolicy);
      }
      if (url.pathname === "/api/proxies/5/health/recovery") { persist(); return json(res, 409, { error: "合成旧执行尚未退出", code: "warp_health_recovery_required" }); }
      if (url.pathname === "/api/proxy-groups" && req.method === "GET") return json(res, 200, { groups });
      if (url.pathname === "/api/proxy-groups" && req.method === "POST") {
        const group = { id: String(groups.length + 1), name: input.name, enabled: input.enabled, revision: 0, members: [] }; groups.push(group); persist(); return json(res, 201, group);
      }
      const groupMatch = url.pathname.match(/^\/api\/proxy-groups\/(\d+)(\/members)?$/);
      if (groupMatch) {
        const group = groups.find((value) => value.id === groupMatch[1]);
        if (!group) return json(res, 404, { error: "分组不存在" });
        if (req.method === "GET") return json(res, 200, group);
        const revision = req.method === "DELETE" ? Number(url.searchParams.get("expected_revision")) : input.expected_revision;
        if (behavior.groupConflict || revision !== group.revision) return json(res, 409, { error: "合成分组版本冲突", code: "proxy_group_revision_conflict", current_revision: group.revision });
        if (req.method === "DELETE") { groups = groups.filter((value) => value.id !== group.id); persist(); return json(res, 204); }
        if (groupMatch[2]) group.members = input.proxy_ids.map((id, position) => ({ proxy_id: id, position, proxy: proxies.find((value) => String(value.id) === id) }));
        else { group.name = input.name; group.enabled = input.enabled; }
        group.revision++; persist(); return json(res, 200, group);
      }
      const subject = url.pathname.match(/^\/api\/(tasks|conversations)\/(\d+)\/proxy(?:\/(?:policy-recovery|switch-requests(?:\/(\d+))?))?$/);
      if (subject) {
        let current = subject[1] === "tasks" ? policy : conversationPolicy;
        if (req.method === "GET") {
          if (behavior.readFailure) { behavior.readFailure = false; return json(res, 503, { error: "合成回读断网" }); }
          return json(res, 200, subject[3] ? current.latest_switch : current);
        }
        if (behavior.delay) await new Promise((resolveDelay) => setTimeout(resolveDelay, behavior.delay));
        if (behavior.writeFailure) return json(res, behavior.writeFailure, { error: "合成真实活动未退出或版本冲突", code: behavior.writeFailure === 409 ? "proxy_busy" : "synthetic_unknown" });
        if (input.expected_revision !== current.proxy_revision) return json(res, 409, { error: "版本冲突", code: "proxy_revision_conflict", current_revision: current.proxy_revision });
        const recovery = url.pathname.endsWith("/policy-recovery");
        if (recovery && (input.action === "retry" || input.action === "confirm_released")) {
          current.recovery_required = false;
          current.proxy_revision++;
          if (current.latest_switch) current.latest_switch = { ...current.latest_switch, state: "succeeded", completed_at: stamp(), result_revision: current.proxy_revision };
          persist();
          if (behavior.unknownAfterCommit) return json(res, 500, { error: "合成恢复提交后未知" });
          return json(res, 200, current);
        }
        if (req.method === "POST" && !recovery) {
          current.latest_switch = { id: "1", subject_kind: current.subject_kind, subject_id: current.subject_id, base_revision: current.proxy_revision, authorization_revision: current.authorization_revision, state: "pending", from_proxy_id: current.proxy_id, to_proxy_id: input.target_proxy_id ?? "2", reason: input.reason, error_code: "", created_at: stamp(), updated_at: stamp(), completed_at: null, result_revision: null };
          persist(); return json(res, 202, { accepted: true, request: current.latest_switch, end_turn: true });
        }
        const id = input.proxy_mode === "group" ? "1" : input.proxy_mode === "fixed" ? input.proxy_id : input.proxy_mode === "inherit" ? "1" : null;
        current = { ...current, ...input, proxy_id: id, proxy: proxies.find((value) => String(value.id) === id) ?? null, proxy_revision: current.proxy_revision + 1, authorization_revision: current.authorization_revision + 1, exit_verified_at: id ? stamp() : null, latest_switch: null, recovery_required: false };
        delete current.expected_revision;
        delete current.action;
        if (subject[1] === "tasks") policy = current; else conversationPolicy = current;
        persist();
        if (behavior.unknownAfterCommit) return json(res, 500, { error: "合成提交后响应未知" });
        return json(res, 200, subject[1] === "tasks" && !recovery ? task() : current);
      }
      if (url.pathname === "/api/tasks/12") return json(res, 200, task());
      if (url.pathname === "/api/conversations") return json(res, 200, { conversations: [{ id: 8, agent_key: "mainagent", title: "合成对话", running: false, created_at: stamp(), updated_at: stamp() }] });
      if (url.pathname === "/api/agents") return json(res, 200, { agents: [{ key: "mainagent", name: "合成 Agent", builtin: true }] });
      if (url.pathname === "/api/auth/status") return json(res, 200, { initialized: true, authenticated: true });
      if (url.pathname.includes("/events") || url.pathname.includes("/stream")) { res.writeHead(200, { "Content-Type": "text/event-stream" }); res.end(); return; }
      if (url.pathname === "/api/llm/profiles") return json(res, 200, { profiles: [] });
      return json(res, 200, { tasks: [], rows: [], records: [], items: [], activity: [], intents: [], sessions: [], findings: [], approvals: [], messages: [], groups: [] });
    }
    const staticRoot = resolve(root, "out");
    let file = resolve(staticRoot, `.${decodeURIComponent(url.pathname)}`, url.pathname.endsWith("/") ? "index.html" : "");
    // Next 分段预取请求使用点分路径，静态导出对应嵌套目录；仅在原路径不存在时按实际产物映射。
    if (!existsSync(file)) {
      const segment = decodeURIComponent(url.pathname).match(/^(.*\/)__next\.(.+)\.txt$/);
      if (segment) file = resolve(staticRoot, `.${segment[1]}__next.${segment[2].replaceAll(".", "/")}.txt`);
    }
    if (!file.startsWith(staticRoot + sep) || !existsSync(file)) return json(res, 404, { error: "not found" });
    const types = { ".html": "text/html; charset=utf-8", ".js": "application/javascript", ".css": "text/css", ".png": "image/png", ".woff2": "font/woff2", ".txt": "text/plain" };
    res.writeHead(200, { "Content-Type": types[extname(file)] ?? "application/octet-stream" }); createReadStream(file).pipe(res);
  } catch (error) { json(res, 500, { error: error.message }); }
});
server.listen(port, "127.0.0.1", () => console.log(`合成 UI 服务：http://127.0.0.1:${port}/__fixture/bootstrap`));
