import { warpReady } from "./proxy-center";
import type { ProxyRecord, Task, TaskProxyMode } from "./types";

export type TaskProxyDraft = { mode: TaskProxyMode | ""; id: string };
export type TaskProxyInput = { proxy_mode: TaskProxyMode; proxy_id: number | null; expected_revision: number };

export function validProxyRevision(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

export function proxyRevisionConflict(error: unknown): boolean {
  const failure = error as { status?: number; payload?: { code?: string } };
  return failure?.status === 409 && failure.payload?.code === "proxy_revision_conflict";
}

export function taskProxyDraft(task: Task): TaskProxyDraft {
  const mode = task.proxy_mode;
  const id = task.proxy_id ?? task.proxy?.id;
  return {
    mode: mode === "inherit" || mode === "direct" || mode === "fixed" ? mode : "",
    id: typeof id === "number" && id > 0 ? String(id) : "",
  };
}

export function taskProxyInput(draft: TaskProxyDraft, revision: unknown): TaskProxyInput {
  if (!validProxyRevision(revision)) throw new Error("任务代理版本缺失或无效，请重新读取任务后再切换");
  if (draft.mode === "inherit" || draft.mode === "direct")
    return { proxy_mode: draft.mode, proxy_id: null, expected_revision: revision };
  const id = Number(draft.id);
  if (draft.mode !== "fixed" || !/^[1-9]\d*$/.test(draft.id) || !Number.isSafeInteger(id)) {
    throw new Error("请明确选择连接方式与有效的代理编号");
  }
  return { proxy_mode: "fixed", proxy_id: id, expected_revision: revision };
}

// 切换现有任务比新建目录展示更严格；unknown 不能被当作健康代理选入。
export function taskProxySwitchSelectable(item: ProxyRecord): boolean {
  return (
    item.enabled &&
    item.status === "healthy" &&
    ["http", "https", "socks5", "warp", "resin"].includes(item.kind) &&
    (item.kind !== "warp" || warpReady(item))
  );
}

export function taskProxyOptionLabel(item: ProxyRecord): string {
  const ip = taskProxySwitchSelectable(item) && item.egress_ip?.trim() ? item.egress_ip : "未验证";
  return `#${item.id} ${item.name} · ${item.kind.toUpperCase()} · 出口 IP：${ip}`;
}

export function taskProxySwitchError(input: {
  task: Task;
  draft: TaskProxyDraft;
  items: ProxyRecord[];
  globalID: number | null;
  loading: boolean;
  error: string;
}): string {
  const { task } = input;
  if (task.queued || task.status === "queued" || (task.status === "running" && !task.paused)) {
    return "任务运行或排队中，请先暂停任务并等待操作退出，再切换代理。";
  }
  if ((task.in_flight ?? 0) > 0) return "仍有操作在途，请等待操作退出；暂停任务不代表工具已经退出。";
  if (input.loading) return "代理目录正在加载，请稍候。";
  if (input.error) return "代理目录加载失败，请刷新目录后再保存，当前选择保持不变。";
  let data: TaskProxyInput;
  try {
    data = taskProxyInput(input.draft, input.task.proxy_revision);
  } catch (error) {
    return (error as Error).message;
  }
  const id = data.proxy_mode === "inherit" ? input.globalID : data.proxy_id;
  if (id !== null) {
    const item = input.items.find((item) => item.id === id);
    if (!item || !taskProxySwitchSelectable(item)) return `代理 #${id} 已不可用或尚未验证，请检测或重新选择。`;
  }
  return "";
}

type Client = { write: (id: string, input: TaskProxyInput) => Promise<unknown>; read: (id: string) => Promise<Task> };

// 保存与回读是两个阶段；PUT 已返回后重试只 GET，不能重复 inherit 重新采样默认出口。
export class TaskProxySubmission {
  busy: boolean;
  needsReadback: boolean;
  writeConfirmed: boolean;
  awaitingConfirmation: boolean;
  constructor() {
    this.busy = false;
    this.needsReadback = false;
    this.writeConfirmed = false;
    this.awaitingConfirmation = false;
  }

  confirmNewIntent(): boolean {
    if (this.busy || this.needsReadback) return false;
    this.awaitingConfirmation = false;
    this.writeConfirmed = false;
    return true;
  }

  async save(id: string, input: TaskProxyInput, client: Client): Promise<Task | null> {
    if (this.busy) return null;
    if (this.awaitingConfirmation && !this.needsReadback) throw new Error("请先确认新的代理选择，不能自动重放旧意图");
    this.busy = true;
    try {
      if (!this.needsReadback) {
        this.writeConfirmed = false;
        try {
          await client.write(id, input);
          this.writeConfirmed = true;
          this.needsReadback = true;
        } catch (error) {
          const status = (error as { status?: number })?.status;
          if (!status || status >= 500 || proxyRevisionConflict(error)) {
            this.needsReadback = true;
            this.awaitingConfirmation = true;
          }
          throw error;
        }
      }
      const task = await client.read(id);
      if (String(task.id) !== id) throw new Error("任务回读编号不一致，请重新核对");
      if (!validProxyRevision(task.proxy_revision)) throw new Error("任务回读缺少有效代理版本，请重新核对");
      this.needsReadback = false;
      return task;
    } finally {
      this.busy = false;
    }
  }
}
