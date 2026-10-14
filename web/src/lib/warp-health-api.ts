import { http } from "./api";
import { validRevision } from "./proxy-policy";
import type { ProxyRecord } from "./types";
import type { WarpHealthPolicy } from "./warp-health";

export const warpHealthAPI = {
  policy: async (id: number) => {
    const value = await http<WarpHealthPolicy>(`/proxies/${id}/health-policy`, { cache: "no-store" });
    if (value.proxy_id !== id || !validRevision(value.revision) || typeof value.auto_recover_enabled !== "boolean")
      throw new Error("健康策略回读编号或版本无效");
    return value;
  },
  save: (id: number, revision: number, enabled: boolean) => {
    if (!validRevision(revision) || typeof enabled !== "boolean") throw new Error("缺少有效健康策略版本或开关状态");
    return http<WarpHealthPolicy>(
      `/proxies/${id}/health-policy`,
      { method: "PUT", body: JSON.stringify({ expected_revision: revision, auto_recover_enabled: enabled }) },
      true,
    );
  },
  // 退出证明只来自服务端。请求只提交恢复意图，绝不发送自证布尔、token 或 revision。
  recover: (id: number) =>
    http<ProxyRecord>(`/proxies/${id}/health/recovery`, { method: "POST", body: JSON.stringify({}) }, true),
  proxy: async (id: number) => {
    const value = await http<ProxyRecord>(`/proxies/${id}`, { cache: "no-store" });
    if (Number(value.id) !== id) throw new Error("WARP 回读编号不一致");
    return value;
  },
};
export type WarpHealthAdapter = typeof warpHealthAPI;
