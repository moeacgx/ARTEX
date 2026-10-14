import type { ProxyKind, ProxyRecord, ProxyWriteInput, Task, TaskProxyMode } from "./types";

export type ProxyForm = {
  name: string;
  kind: ProxyKind;
  protocol: "http" | "socks5";
  url: string;
  host: string;
  port: string;
  password: string;
  platform: string;
  adminURL: string;
  adminToken: string;
  enabled: boolean;
  clearPassword: boolean;
  clearAdminToken: boolean;
};

export function proxyErrorText(error: unknown): string {
  return (error instanceof Error ? error.message : String(error))
    .replace(/:\/\/[^/@\s]+@/g, "://***@")
    .replace(/(bearer\s+)\S+/gi, "$1***")
    .replace(/((?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+/gi, "$1***");
}

export function proxyEndpoint(item: Pick<ProxyRecord, "kind" | "protocol" | "host" | "port" | "url">): string {
  if (item.kind === "warp") return "本机 WireProxy SOCKS5";
  if (item.kind === "resin") return `${item.protocol ?? "socks5"}://${item.host ?? "?"}:${item.port ?? "?"}`;
  if (!item.url) return "地址未返回";
  try {
    const url = new URL(item.url);
    if (url.username || url.password) {
      url.username = "***";
      url.password = "";
    }
    url.search = "";
    url.hash = "";
    return url.toString();
  } catch {
    return "地址格式未知";
  }
}

export function proxySelectable(item: ProxyRecord): boolean {
  if (item.kind === "warp") return warpReady(item);
  return item.enabled && ["unknown", "healthy"].includes(item.status);
}

export function taskProxyError(input: {
  loading: boolean;
  error: string;
  mode: TaskProxyMode;
  id?: number;
  globalID: number | null;
  items: ProxyRecord[];
}): string {
  if (input.loading) return "代理目录仍在加载，请稍候";
  if (input.error) return "代理目录加载失败，重试成功后才能创建任务";
  let id: number | null | undefined = null;
  if (input.mode === "fixed") id = input.id;
  if (input.mode === "inherit") id = input.globalID;
  if (input.mode === "fixed" && !id) return "请选择代理编号";
  if (id !== null && id !== undefined) {
    const proxy = input.items.find((item) => item.id === id);
    if (!proxy || !proxySelectable(proxy)) return `代理 #${id} 已不可用，请修复或重新选择`;
  }
  return "";
}

export function buildProxyPayload(form: ProxyForm, editing: boolean): ProxyWriteInput {
  const name = form.name.trim();
  if (!name) throw new Error("请填写代理名称");
  // 点击“确认添加”即明确接受条款；WARP 新建一次完成注册、启动和出口验证。
  if (form.kind === "warp" && !editing) return { name, kind: "warp", accept_terms: true };
  const data: ProxyWriteInput = { name, kind: form.kind, enabled: form.enabled };
  if (form.kind === "resin") {
    const port = Number(form.port);
    if (!form.host.trim() || !Number.isInteger(port) || port < 1 || port > 65535) {
      throw new Error("请填写有效的数据面 Host 和 Port");
    }
    if (!editing && !form.password) throw new Error("请填写 Resin Proxy Token");
    data.host = form.host.trim();
    data.port = port;
    data.protocol = form.protocol;
    data.resin_platform = form.platform.trim() || "Default";
    data.resin_admin_url = form.adminURL.trim();
  } else if (form.kind !== "warp") {
    const url = form.url.trim();
    if (!editing && !url) throw new Error("请填写代理 URL");
    if (url) {
      try {
        const parsed = new URL(url);
        if (parsed.protocol !== `${form.kind}:` || !parsed.hostname) throw new Error();
      } catch {
        throw new Error(`代理 URL 必须是有效的 ${form.kind}:// 地址`);
      }
      data.url = url;
    }
  }
  if (form.kind !== "warp") {
    if (editing && form.clearPassword) data.clear_password = true;
    else if (form.password) data.password = form.password;
  }
  if (form.kind === "resin") {
    if (editing && form.clearAdminToken) data.clear_resin_admin_token = true;
    else if (form.adminToken) data.resin_admin_token = form.adminToken;
  }
  return data;
}

export function taskProxyLabel(task: Pick<Task, "proxy_mode" | "proxy_id" | "proxy">): string {
  if (task.proxy) return `代理 #${task.proxy.id}`;
  if (task.proxy_mode === "group") return "分组出口待核对";
  if (task.proxy_mode === "direct") return "直连";
  if (task.proxy_mode === "inherit" && !task.proxy_id) return "默认直连";
  return "代理快照缺失";
}

// ─────────────────────────────────────────────────────────────────────────────
// WARP 注册条款确认
// ─────────────────────────────────────────────────────────────────────────────

// 只有 WARP 「注册」这一步需要产品用户先明确同意 Cloudflare 的注册使用条款，
// 后端要求请求体必须带 accept_terms:true，缺失或 false 都拒绝。start/stop/recover
// 只是对本机已有档案的启停，不涉及注册条款，绝不能混同。
export type WarpActionKind = "register" | "start" | "stop" | "recover";

export function warpActionNeedsTerms(action: WarpActionKind): boolean {
  return action === "register";
}

export const WARP_TERMS_URL = "https://www.cloudflare.com/application/terms/";

export function warpReady(item: ProxyRecord): boolean {
  if (item.warp_health) {
    const health = item.warp_health;
    return (
      item.kind === "warp" &&
      item.enabled &&
      item.status === "healthy" &&
      health.proxy_id === Number(item.id) &&
      health.desired_running &&
      health.state === "ready" &&
      !!health.egress_ip?.trim() &&
      !!health.last_checked_at
    );
  }
  return (
    item.kind === "warp" && item.enabled && item.warp_phase === "ready" && item.status === "healthy" && !!item.egress_ip
  );
}

export function warpAllowedActions(item: ProxyRecord): WarpActionKind[] {
  if (item.warp_health && ["unknown", "pending", "repairing"].includes(item.warp_health.state)) return [];
  switch (item.warp_phase) {
    case "unregistered":
      return ["register"];
    case "ready":
      return warpReady(item) ? [] : ["recover"];
    case "stopped":
      return ["start", "recover"];
    case "failed":
    case "generating":
      return ["recover"];
    default:
      return [];
  }
}

export const WARP_PHASE_LABEL: Record<string, string> = {
  unregistered: "未注册（旧条目）",
  registering: "注册中",
  generating: "生成连接配置中",
  starting: "连接中",
  ready: "已连接",
  stopped: "已停止",
  failed: "连接失败",
};

// 只认错误包中的已建档 ID，不按名称猜测，也不因 502 再发创建请求。
export function proxyFromFailure(error: unknown): ProxyRecord | null {
  const payload = (error as { payload?: { proxy_id?: unknown; proxy?: ProxyRecord } })?.payload;
  const item = payload?.proxy;
  return item && Number.isInteger(item.id) && item.id > 0 && payload?.proxy_id === item.id ? item : null;
}

// warpRegisterBody：注册前必须已由用户勾选确认；未确认时返回 null，调用方【不得】
// 发送请求（不是发送 accept_terms:false）。确认后固定携带 accept_terms:true。
export function warpRegisterBody(accepted: boolean): { accept_terms: true } | null {
  return accepted ? { accept_terms: true } : null;
}

// 条款文案集中在此，供确认框展示；措辞对齐 docs/proxy-runtime.md 的注册说明，
// 不对外编造条款链接或第三方地址。
export const WARP_REGISTER_TERMS = [
  "注册会用本机 wgcf 向 Cloudflare 申请一个 WARP 免费账号，并把该档案（含凭据）保存在本机目录，权限 0600。",
  "注册请求一旦发出是否成功由服务端返回决定；若返回结果不确定，系统会记录“已尝试注册”并拒绝自动重复注册，需要你手动确认后再重试。",
  "注册过程不修改宿主机默认路由，只提供一个供 ARTEX 使用的本机 SOCKS5 出口。",
] as const;
