"use client";

import * as React from "react";

import { Badge } from "@/components/ui/badge";
import { proxyErrorText } from "@/lib/proxy-center";
import {
  cooldownRemaining,
  effectiveExit,
  type ProxyPolicy,
  policyRecoveryRequired,
  SWITCH_LABEL,
} from "@/lib/proxy-policy";

export function ProxyPolicyStatus({ policy, uncertain = false }: { policy: ProxyPolicy; uncertain?: boolean }) {
  const [now, setNow] = React.useState(() => Date.now());
  React.useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const ip = effectiveExit(policy, uncertain),
    request = policy.latest_switch;
  const cooldown = cooldownRemaining(policy.cooldown_until, now);
  return (
    <div className="space-y-3 rounded-lg border bg-muted/30 p-3" aria-live="polite">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="font-medium">实际当前出口</p>
        <Badge variant="outline">版本 {policy.proxy_revision}</Badge>
      </div>
      <p>
        {uncertain
          ? "状态待核对"
          : policy.proxy
            ? `#${policy.proxy.id} ${policy.proxy.name} · ${policy.proxy.kind.toUpperCase()}`
            : policy.proxy_mode === "direct" || policy.proxy_mode === "inherit"
              ? "直连快照"
              : "无有效出口，已阻止直连回落"}
      </p>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
        <div>
          <dt className="text-muted-foreground">真实有效出口 IP</dt>
          <dd className="break-all font-mono">{ip ?? "未验证"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">有效验证时间</dt>
          <dd>{ip && policy.exit_verified_at ? new Date(policy.exit_verified_at).toLocaleString() : "—"}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">冷却剩余</dt>
          <dd>{cooldown} 秒</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">剩余切换次数</dt>
          <dd>
            {policy.remaining_switches} / {policy.failover_max_switches}
          </dd>
        </div>
      </dl>
      {policyRecoveryRequired(policy) && !request && (
        <p className="text-destructive text-xs">待人工恢复：持久屏障仍在，需使用正式恢复流程；刷新不会解除。</p>
      )}
      {request && (
        <div className="space-y-1 border-t pt-2 text-xs">
          <Badge
            variant={request.state === "failed" || request.state === "recovery_required" ? "destructive" : "secondary"}
          >
            {SWITCH_LABEL[request.state]}
          </Badge>
          <p>最近切换原因：{proxyErrorText(request.reason || "未提供")}</p>
          {request.error_code && (
            <p className="text-destructive">
              {request.error_code === "proxy_candidates_exhausted"
                ? "候选已耗尽，需人工处理，不回落直连"
                : proxyErrorText(request.error_code)}
            </p>
          )}
          {request.state === "recovery_required" && (
            <p>需核对原执行与进程是否真实退出，再使用正式人工恢复流程；刷新状态不会解除屏障。</p>
          )}
        </div>
      )}
    </div>
  );
}
