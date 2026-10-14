import type { ProxyRecord } from "./types";

export function warpEffectiveIP(proxy: ProxyRecord): string | null {
  const health = proxy.warp_health;
  if (
    !health ||
    proxy.kind !== "warp" ||
    !proxy.enabled ||
    proxy.status !== "healthy" ||
    health.proxy_id !== proxy.id ||
    !health.desired_running ||
    health.state !== "ready" ||
    !health.last_checked_at ||
    !Number.isFinite(Date.parse(health.last_checked_at))
  )
    return null;
  return health.egress_ip?.trim() || null;
}

export type WarpHealthPolicy = {
  proxy_id: string;
  revision: number;
  auto_recover_enabled: boolean;
  probe_interval_seconds: 60;
  failure_threshold: 3;
  recovery_min_seconds: 300;
  recovery_max_seconds: 3600;
};
