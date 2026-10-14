import { proxyFromFailure } from "./proxy-center";
import type { ProxyRecord } from "./types";

type Failure =
  | { stage: "pending"; id: string | null; proxy: ProxyRecord | null }
  | { stage: "retained"; proxy: ProxyRecord }
  | { stage: "deleted" | "conflict" | "uncertain" };

export function warpCreateFailure(error: unknown): Failure {
  const { status, payload } = (error ?? {}) as {
    status?: number;
    payload?: { operation_status?: string; proxy_id?: string };
  };
  // 持久化完成结果失败也可能返回 502 pending；不能按 DTO phase 猜测可恢复。
  if (status === 202 || payload?.operation_status === "pending") {
    const id = payload?.proxy_id;
    return {
      stage: "pending",
      id: typeof id === "string" && /^[1-9]\d*$/.test(id) ? id : null,
      proxy: proxyFromFailure(error),
    };
  }
  if (status === 410) return { stage: "deleted" };
  if (status === 409) return { stage: "conflict" };
  const proxy = proxyFromFailure(error);
  if (proxy) return { stage: "retained", proxy };
  return { stage: "uncertain" };
}
