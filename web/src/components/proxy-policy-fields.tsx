"use client";

import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  groupUnavailable,
  type PolicyDraft,
  type ProxyGroup,
  proxyUnavailable,
  type Subject,
} from "@/lib/proxy-policy";
import type { ProxyRecord } from "@/lib/types";

export function ProxyPolicyFields({
  subject,
  draft,
  onChange,
  groups,
  proxies,
  disabled,
  idPrefix,
}: {
  subject: Subject;
  draft: PolicyDraft;
  onChange: (draft: PolicyDraft) => void;
  groups: ProxyGroup[];
  proxies: ProxyRecord[];
  disabled: boolean;
  idPrefix?: string;
}) {
  const prefix = idPrefix ?? `${subject.kind}-${subject.id}-proxy`;
  return (
    <fieldset disabled={disabled} className="min-w-0 space-y-4 disabled:opacity-60">
      <div className="grid gap-2">
        <Label htmlFor={`${prefix}-mode`}>连接方式</Label>
        <select
          id={`${prefix}-mode`}
          className="h-10 w-full min-w-0 rounded-md border border-input bg-background px-3"
          value={draft.mode}
          onChange={(event) => onChange({ ...draft, mode: event.target.value as PolicyDraft["mode"] })}
        >
          <option value="inherit">继承当前全局默认</option>
          <option value="direct">明确直连</option>
          <option value="fixed">指定代理</option>
          <option value="group">使用获准分组</option>
        </select>
        {draft.mode === "inherit" && (
          <p className="text-muted-foreground text-xs">保存时明确采样当前全局默认；打开或刷新不会改变原快照。</p>
        )}
        {draft.mode === "direct" && (
          <p className="text-muted-foreground text-xs">明确使用直连。候选耗尽或恢复失败不会自动切到此模式。</p>
        )}
      </div>
      {draft.mode === "fixed" && (
        <div className="grid gap-2">
          <Label htmlFor={`${prefix}-fixed`}>指定代理编号</Label>
          <select
            id={`${prefix}-fixed`}
            className="h-10 w-full min-w-0 rounded-md border border-input bg-background px-3"
            value={draft.proxyID}
            onChange={(event) => onChange({ ...draft, proxyID: event.target.value })}
          >
            <option value="">请选择代理</option>
            {draft.proxyID && !proxies.some((proxy) => String(proxy.id) === draft.proxyID) && (
              <option value={draft.proxyID}>#{draft.proxyID}（记录缺失）</option>
            )}
            {proxies.map((proxy) => {
              const reason = proxyUnavailable(proxy, subject.kind === "task" ? "fixed-task" : "fixed-conversation");
              return (
                <option key={proxy.id} value={String(proxy.id)} disabled={!!reason}>
                  #{proxy.id} {proxy.name} · {proxy.kind.toUpperCase()}
                  {reason ? ` · ${reason}` : ""}
                </option>
              );
            })}
          </select>
          {proxies
            .filter((proxy) => proxy.kind === "resin" && subject.kind === "conversation")
            .map((proxy) => (
              <p key={proxy.id} className="text-muted-foreground text-xs">
                #{proxy.id}：{proxyUnavailable(proxy, "fixed-conversation")}
              </p>
            ))}
        </div>
      )}
      <div className="space-y-2 rounded-lg border p-3">
        <p className="font-medium">本{subject.kind === "task" ? "任务" : "对话"}使用分组的授权</p>
        <p className="text-muted-foreground text-xs">
          管理分组只维护目录，不授予此主体使用权限。以下勾选由你明确授权；切换方式后仍保留勾选，取消勾选可撤权。
        </p>
        {groups.length === 0 && <p className="text-muted-foreground text-sm">尚无分组，请先在代理中心创建。</p>}
        <div className="max-h-48 space-y-2 overflow-y-auto">
          {groups.map((group) => {
            const reason = groupUnavailable(group),
              checked = draft.groupIDs.includes(group.id);
            return (
              <Label key={group.id} className="flex items-start gap-2 rounded-md border p-2 font-normal">
                <Checkbox
                  checked={checked}
                  disabled={disabled || (!!reason && !checked)}
                  onCheckedChange={(value) =>
                    onChange({
                      ...draft,
                      groupIDs:
                        value === true ? [...draft.groupIDs, group.id] : draft.groupIDs.filter((id) => id !== group.id),
                    })
                  }
                />
                <span className="min-w-0">
                  <span>
                    #{group.id} {group.name} · {group.members.length} 个成员
                  </span>
                  {reason && <span className="text-destructive block text-xs">{reason}</span>}
                  {group.members
                    .filter((member) => member.proxy.kind === "resin")
                    .map((member) => (
                      <span key={member.proxy_id} className="text-muted-foreground block text-xs">
                        #{member.proxy_id}：{proxyUnavailable(member.proxy, "group")}
                      </span>
                    ))}
                </span>
              </Label>
            );
          })}
          {draft.groupIDs
            .filter((id) => !groups.some((group) => group.id === id))
            .map((id) => (
              <Label key={id} className="flex gap-2 font-normal">
                <Checkbox
                  checked
                  onCheckedChange={() =>
                    onChange({ ...draft, groupIDs: draft.groupIDs.filter((value) => value !== id) })
                  }
                />
                #{id}（分组记录缺失；可撤销授权）
              </Label>
            ))}
        </div>
      </div>
      <div className="space-y-3 rounded-lg border p-3">
        <div className="flex items-center justify-between gap-3">
          <Label htmlFor={`${prefix}-agent`}>允许 Agent 在获准组内故障切换</Label>
          <Switch
            id={`${prefix}-agent`}
            checked={draft.agentEnabled}
            disabled={disabled}
            onCheckedChange={(agentEnabled) => onChange({ ...draft, agentEnabled })}
          />
        </div>
        <p className="text-muted-foreground text-xs">
          默认关闭。开启只授权基础设施故障恢复，不扩大组权限，不自动重放有副作用操作。
        </p>
        <div className="grid grid-cols-2 gap-3">
          <div className="grid gap-2">
            <Label htmlFor={`${prefix}-cooldown`}>冷却（秒）</Label>
            <Input
              id={`${prefix}-cooldown`}
              type="number"
              min={0}
              max={86400}
              value={draft.cooldown}
              onChange={(event) => onChange({ ...draft, cooldown: event.target.value })}
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor={`${prefix}-budget`}>切换次数预算</Label>
            <Input
              id={`${prefix}-budget`}
              type="number"
              min={0}
              max={1000}
              value={draft.budget}
              onChange={(event) => onChange({ ...draft, budget: event.target.value })}
            />
          </div>
        </div>
      </div>
      <p className="text-muted-foreground text-xs">
        403、验证码属于目标站点限制；429 应按 Retry-After 等待，不能通过轮换出口规避。
      </p>
    </fieldset>
  );
}
