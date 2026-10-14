"use client";

import * as React from "react";

import { ProxyPolicyFields } from "@/components/proxy-policy-fields";
import { ProxyPolicyStatus } from "@/components/proxy-policy-status";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { api } from "@/lib/api";
import { proxyErrorText } from "@/lib/proxy-center";
import {
  draftFromPolicy,
  type PolicyDraft,
  type PolicyRecoveryAction,
  type ProxyGroup,
  type ProxyPolicy,
  policyDraftError,
  policyRecoveryInput,
  policyRecoveryMatches,
  policyRecoveryRequired,
  RevisionSubmission,
  recoveryConfirmsLease,
  recoveryReplacesPolicy,
  type Subject,
  switchPending,
} from "@/lib/proxy-policy";
import type { ProxyPolicyAdapter } from "@/lib/proxy-policy-api";
import type { ProxyRecord } from "@/lib/types";

export function ProxyPolicyRecovery({
  subject,
  adapter,
  disabled,
  onSaving,
  onUpdated,
}: {
  subject: Subject;
  adapter: ProxyPolicyAdapter;
  disabled: boolean;
  onSaving?: () => void;
  onUpdated: (policy: ProxyPolicy) => void;
}) {
  const [open, setOpen] = React.useState(false);
  const [policy, setPolicy] = React.useState<ProxyPolicy | null>(null);
  const [draft, setDraft] = React.useState<PolicyDraft | null>(null);
  const [expected, setExpected] = React.useState<number | null>(null);
  const [action, setAction] = React.useState<PolicyRecoveryAction>("retry");
  const [leaseChecked, setLeaseChecked] = React.useState(false);
  const [groups, setGroups] = React.useState<ProxyGroup[]>([]);
  const [proxies, setProxies] = React.useState<ProxyRecord[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState("");
  const [directoryError, setDirectoryError] = React.useState("");
  const [review, setReview] = React.useState(false);
  const [readback, setReadback] = React.useState(false);
  const [submission] = React.useState(() => new RevisionSubmission<ProxyPolicy>());
  const sequence = React.useRef(0);
  const mounted = React.useRef(true);
  React.useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      sequence.current++;
    };
  }, []);
  const sync = () => {
    setReview(submission.reviewRequired);
    setReadback(submission.needsReadback);
  };
  async function load() {
    const current = ++sequence.current;
    setLoading(true);
    setDirectoryError("");
    const results = await Promise.allSettled([adapter.policy(subject), adapter.groups(), api.proxies()]);
    if (!mounted.current || current !== sequence.current) return;
    const [state, directory, proxyDirectory] = results;
    if (state.status === "fulfilled") {
      setPolicy(state.value);
      if (!submission.reviewRequired && !submission.needsReadback) {
        setDraft(draftFromPolicy(state.value));
        setExpected(state.value.proxy_revision);
        setAction("retry");
        setLeaseChecked(false);
        setError("");
      }
    } else {
      setPolicy(null);
      setError(`恢复策略读取失败：${proxyErrorText(state.reason)}`);
    }
    if (directory.status === "fulfilled") setGroups(directory.value);
    else setDirectoryError(`分组目录读取失败：${proxyErrorText(directory.reason)}`);
    if (proxyDirectory.status === "fulfilled") setProxies(proxyDirectory.value.proxies);
    else setDirectoryError(`代理目录读取失败：${proxyErrorText(proxyDirectory.reason)}`);
    setLoading(false);
  }
  function changeOpen(value: boolean) {
    if (submission.busy) return;
    setOpen(value);
    if (value) void load();
    else {
      sequence.current++;
      setLoading(false);
    }
  }
  const replaces = recoveryReplacesPolicy(action);
  const confirmsLease = recoveryConfirmsLease(action);
  const hasResin = proxies.some(
    (proxy) =>
      proxy.kind === "resin" &&
      (String(proxy.id) === policy?.latest_switch?.from_proxy_id || String(proxy.id) === policy?.proxy_id),
  );
  let blocked = "";
  if (!policy || !draft || expected === null) blocked = "请先读取权威策略版本";
  else if (!policyRecoveryRequired(policy)) blocked = "当前没有待恢复屏障；不会重复提交原恢复";
  else if (switchPending(policy.latest_switch)) blocked = "原请求仍在处理中，请等待真实操作退出";
  else if (confirmsLease && (!hasResin || !leaseChecked)) blocked = "须先人工核对远端原 Resin 租约已释放";
  else if (replaces) blocked = directoryError || policyDraftError(draft, expected, subject, groups, proxies);

  async function save() {
    if (submission.busy || blocked || review || readback || !draft || expected === null) return;
    const current = sequence.current;
    const input = policyRecoveryInput(action, expected, draft, leaseChecked);
    setSaving(true);
    setError("");
    onSaving?.();
    try {
      const value = await submission.submit(
        async () => {
          await adapter.recover(subject, input);
        },
        () => adapter.policy(subject),
        (result) => policyRecoveryMatches(result, input),
      );
      if (!mounted.current || current !== sequence.current || !value) return;
      setPolicy(value);
      onUpdated(value);
      setError(
        submission.confirmed
          ? "人工恢复已回读确认，当前出口以服务端状态为准。"
          : "恢复尚未确认，草稿保留；请仅刷新核对。不会强制切直连。",
      );
    } catch (failure) {
      if (mounted.current && current === sequence.current)
        setError(`恢复未确认：${proxyErrorText(failure)}。草稿保留，只允许 GET 核对，不重复提交恢复。`);
    } finally {
      if (mounted.current && current === sequence.current) {
        setSaving(false);
        sync();
      }
    }
  }
  async function reconcile() {
    if (submission.busy) return;
    const current = sequence.current;
    setSaving(true);
    try {
      const value = await submission.reconcile(() => adapter.policy(subject));
      if (!mounted.current || current !== sequence.current || !value) return;
      setPolicy(value);
      onUpdated(value);
      setError("已 GET 核对当前屏障与版本，原恢复草稿保留。采用当前版本后才可提交新意图。");
    } catch (failure) {
      if (mounted.current && current === sequence.current) {
        submission.needsReadback = true;
        setError(`核对失败：${proxyErrorText(failure)}。不会重发恢复。`);
      }
    } finally {
      if (mounted.current && current === sequence.current) {
        setSaving(false);
        sync();
      }
    }
  }
  const controlsDisabled = loading || saving || readback || review;
  return (
    <>
      <Button variant="outline" disabled={disabled} onClick={() => changeOpen(true)}>
        人工恢复出口策略
      </Button>
      <Dialog open={open} onOpenChange={changeOpen}>
        <DialogContent className="grid-cols-1 sm:max-w-2xl" showCloseButton={!saving}>
          <DialogHeader>
            <DialogTitle>人工恢复{subject.kind === "task" ? "任务" : "对话"}出口策略</DialogTitle>
            <DialogDescription>
              恢复由服务端检查真实活动、清理退出与版本。默认恢复原持久目标，不重新采样继承出口、不重置预算、不重放业务操作。
            </DialogDescription>
          </DialogHeader>
          {loading && <p role="status">正在读取权威恢复状态…</p>}
          {policy && <ProxyPolicyStatus policy={policy} uncertain={loading || readback} />}
          <div className="grid gap-2">
            <Label htmlFor={`${subject.kind}-${subject.id}-recovery-action`}>恢复操作</Label>
            <select
              id={`${subject.kind}-${subject.id}-recovery-action`}
              className="h-10 w-full min-w-0 rounded-md border bg-background px-3"
              value={action}
              disabled={controlsDisabled}
              onChange={(event) => {
                setAction(event.target.value as PolicyRecoveryAction);
                setLeaseChecked(false);
              }}
            >
              <option value="retry">恢复原持久目标</option>
              <option value="replace_policy">明确提交新的出口策略</option>
              {hasResin && <option value="confirm_released">人工核对原 Resin 租约已释放，再恢复原目标</option>}
              {hasResin && (
                <option value="confirm_released_replace_policy">人工核对原 Resin 租约已释放，并提交新策略</option>
              )}
            </select>
          </div>
          {confirmsLease && (
            <Label className="flex items-start gap-2 rounded-lg border p-3 font-normal">
              <Checkbox
                checked={leaseChecked}
                disabled={controlsDisabled}
                onCheckedChange={(value) => setLeaseChecked(value === true)}
              />
              <span>
                我已人工核对远端原租约确已释放。此操作不再次
                DELETE；服务端仍须校验原材料身份和真实空闲。此勾选不证明进程退出。
              </span>
            </Label>
          )}
          {replaces && draft && (
            <ProxyPolicyFields
              subject={subject}
              draft={draft}
              onChange={setDraft}
              groups={groups}
              proxies={proxies}
              disabled={controlsDisabled}
              idPrefix={`${subject.kind}-${subject.id}-recovery`}
            />
          )}
          {blocked && (
            <p role="status" className="text-destructive text-sm">
              {blocked}
            </p>
          )}
          {error && (
            <p role="status" className="text-sm">
              {error}
            </p>
          )}
          {review && !readback && policy && (
            <Button
              variant="outline"
              disabled={saving || loading}
              onClick={() => {
                if (submission.confirmNewIntent()) {
                  setExpected(policy.proxy_revision);
                  sync();
                  setError("已采用当前版本，恢复草稿保留；再次提交才会生效。");
                }
              }}
            >
              核对完成并采用恢复版本
            </Button>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={saving} onClick={() => changeOpen(false)}>
              关闭恢复
            </Button>
            <Button variant="outline" disabled={saving || loading} onClick={() => void reconcile()}>
              仅 GET 核对恢复
            </Button>
            <Button disabled={controlsDisabled || !!blocked} onClick={() => void save()}>
              提交人工策略恢复
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
