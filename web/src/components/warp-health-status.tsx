"use client";

import { Badge } from "@/components/ui/badge";
import { proxyErrorText } from "@/lib/proxy-center";
import type { ProxyRecord, WarpHealth } from "@/lib/types";
import { warpEffectiveIP } from "@/lib/warp-health";

const labels: Record<WarpHealth["state"], string> = {
  unknown: "状态未知，需人工核对",
  stopped: "已停止",
  disabled: "已停用",
  pending: "创建或注册待确认",
  checking: "正在检测",
  ready: "已验证在线",
  failed: "连接故障",
  deferred: "等待安全空闲",
  repairing: "正在恢复原出口",
};
const time = (value?: string) =>
  value && Number.isFinite(Date.parse(value)) ? new Date(value).toLocaleString() : "尚未发生";
export function WarpHealthStatus({ proxy }: { proxy: ProxyRecord }) {
  const health = proxy.warp_health,
    ip = warpEffectiveIP(proxy);
  return (
    <div className="w-full space-y-3 rounded-lg border bg-muted/20 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="font-medium text-sm">WARP 持续健康检测</p>
        <Badge variant={health?.state === "failed" || health?.state === "unknown" ? "destructive" : "secondary"}>
          {health ? labels[health.state] : "健康信息尚未返回"}
        </Badge>
      </div>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
        <div>
          <dt className="text-muted-foreground">真实有效出口 IP</dt>
          <dd className="break-all font-mono">{ip ?? "未验证"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">明确运行意图</dt>
          <dd>{health ? (health.desired_running ? "保持运行" : "不自动启动") : "未确认"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">最近探测</dt>
          <dd>{time(health?.last_checked_at)}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">下次检测</dt>
          <dd>{time(health?.next_probe_at)}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">连续失败 / 恢复尝试</dt>
          <dd>{health ? `${health.consecutive_failures} / ${health.recovery_attempts}` : "—"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">下次恢复</dt>
          <dd>{time(health?.next_recovery_at)}</dd>
        </div>
      </dl>
      {health?.last_error && <p className="text-destructive text-xs">{proxyErrorText(health.last_error)}</p>}
      {(health?.state === "unknown" || health?.state === "pending") && (
        <p className="text-xs">
          注册或恢复结果尚未确认。请核对原条目与原执行；不会再次注册、创建新出口或自行解除恢复屏障。
        </p>
      )}
      {health?.state === "deferred" && (
        <p className="text-xs">有实际活动或资源尚未退出，恢复推迟；暂停或终态不代表已空闲。</p>
      )}
      <p className="text-muted-foreground text-xs">
        默认每 60 秒探测；连续 3 次失败后复探，仅安全空闲时复用原注册材料重连。失败恢复按 300 → 3600
        秒有界退避；恢复可能保留相同 IP。
      </p>
      <p className="text-muted-foreground text-xs">
        有效 IP 仅表示当前验证结果。服务端尚未提供历史 IP 字段，此处不缓存历史 IP。
      </p>
    </div>
  );
}
