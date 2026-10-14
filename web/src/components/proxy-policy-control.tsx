"use client";

import * as React from "react";

import { RadioTowerIcon } from "lucide-react";

import { ProxyPolicyFields } from "@/components/proxy-policy-fields";
import { ProxyPolicyRecovery } from "@/components/proxy-policy-recovery";
import { ProxyPolicyStatus } from "@/components/proxy-policy-status";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { api } from "@/lib/api";
import { proxyErrorText } from "@/lib/proxy-center";
import {
  allowedCandidates,
  cooldownRemaining,
  draftFromPolicy,
  type FailoverInput,
  type PolicyDraft,
  type ProxyGroup,
  type ProxyPolicy,
  policyDraftError,
  policyInput,
  policyMatches,
  policyRecoveryRequired,
  RevisionSubmission,
  type Subject,
  switchPending,
} from "@/lib/proxy-policy";
import { type ProxyPolicyAdapter, proxyPolicyAPI } from "@/lib/proxy-policy-api";
import type { ProxyRecord } from "@/lib/types";

export function ProxyPolicyControl({
  subject,
  onSaving,
  onUpdated,
  adapter = proxyPolicyAPI,
}: {
  subject: Subject;
  onSaving?: () => void;
  onUpdated?: () => void;
  adapter?: ProxyPolicyAdapter;
}) {
  const label = subject.kind === "task" ? "任务" : "对话";
  const [open, setOpen] = React.useState(false);
  const [policy, setPolicy] = React.useState<ProxyPolicy | null>(null);
  const [draft, setDraft] = React.useState<PolicyDraft | null>(null);
  const [expected, setExpected] = React.useState<number | null>(null);
  const [groups, setGroups] = React.useState<ProxyGroup[]>([]);
  const [proxies, setProxies] = React.useState<ProxyRecord[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const [directoryError, setDirectoryError] = React.useState("");
  const [policyError, setPolicyError] = React.useState("");
  const [saving, setSaving] = React.useState(false);
  const [readback, setReadback] = React.useState(false);
  const [review, setReview] = React.useState(false);
  const [submission] = React.useState(() => new RevisionSubmission<ProxyPolicy>());
  const [reason, setReason] = React.useState("");
  const [reference, setReference] = React.useState("");
  const [evidenceKind, setEvidenceKind] = React.useState<FailoverInput["evidence"]["kind"]>("connection_failure");
  const [target, setTarget] = React.useState("");
  const [now, setNow] = React.useState(() => Date.now());
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
    setReadback(submission.needsReadback);
    setReview(submission.reviewRequired);
  };
  React.useEffect(() => {
    if (!open) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [open]);

  async function load(initialize: boolean) {
    const current = ++sequence.current;
    setLoading(true);
    setDirectoryError("");
    setPolicyError("");
    const results = await Promise.allSettled([adapter.policy(subject), adapter.groups(), api.proxies()]);
    if (!mounted.current || current !== sequence.current) return;
    const [state, directory, proxyDirectory] = results;
    if (state.status === "fulfilled") {
      setPolicy(state.value);
      if (initialize && !submission.reviewRequired && !submission.needsReadback) {
        setDraft(draftFromPolicy(state.value));
        setExpected(state.value.proxy_revision);
        setError("");
      }
    } else {
      setPolicy(null);
      setPolicyError(`策略读取失败：${proxyErrorText(state.reason)}`);
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
    if (value) void load(true);
    else {
      sequence.current++;
      setLoading(false);
    }
  }

  // 轮询只读原请求；关闭、切换主体或迟到响应均不能覆盖新选择。
  const pendingID =
    submission.pending?.id ?? (policy && switchPending(policy.latest_switch) ? policy.latest_switch?.id : null);
  React.useEffect(() => {
    if (!open || !pendingID) return;
    const pollingSubject = { kind: subject.kind, id: subject.id };
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      if (submission.busy) {
        if (active) timer = setTimeout(() => void poll(), 2000);
        return;
      }
      const current = sequence.current;
      try {
        const request = await adapter.switchRequest(pollingSubject, pendingID);
        if (!active || current !== sequence.current) return;
        const next = await adapter.policy(pollingSubject);
        if (!active || current !== sequence.current) return;
        setPolicy(next);
        if (!switchPending(request)) {
          submission.pending = null;
          onUpdated?.();
          return;
        }
      } catch (failure) {
        if (active) setError(`进度待核对：${proxyErrorText(failure)}。不会重发切换请求。`);
      }
      if (active) timer = setTimeout(() => void poll(), 2000);
    };
    timer = setTimeout(() => void poll(), 1000);
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [open, pendingID, adapter, subject.kind, subject.id, submission, onUpdated]);

  let blocked = "";
  if (!draft || expected === null) blocked = "请先读取当前策略";
  else blocked = policyDraftError(draft, expected, subject, groups, proxies);
  if (directoryError) blocked = directoryError;
  if (policyError || !policy) blocked = policyError || "当前策略尚未确认，请仅刷新核对";
  if (switchPending(policy?.latest_switch ?? null)) blocked = "正在处理原切换请求，请等待排空和执行完成";
  if (policyRecoveryRequired(policy)) blocked = "当前处于人工恢复屏障，需通过正式恢复流程核对";

  async function save(requestSwitch = false) {
    if (submission.busy || !draft || expected === null || (!submission.needsReadback && (blocked || review))) return;
    const current = sequence.current;
    setSaving(true);
    setError("");
    onSaving?.();
    try {
      const input = policyInput(draft, expected);
      const result = await submission.submit(
        async () => {
          if (requestSwitch) {
            if (!reason.trim() || reason.trim().length > 500 || !reference.trim())
              throw new Error("请填写原因和服务端运行时证据编号");
            const request = await adapter.request(subject, {
              expected_revision: expected,
              reason: reason.trim(),
              evidence: { kind: evidenceKind, reference: reference.trim() },
              ...(target ? { target_proxy_id: target } : {}),
            });
            return { pending: request };
          }
          await adapter.save(subject, input);
        },
        () => adapter.policy(subject),
        (value) => policyMatches(value, input),
      );
      if (!mounted.current || current !== sequence.current || !result) return;
      setPolicy(result);
      setPolicyError("");
      onUpdated?.();
      if (submission.confirmed) {
        setExpected(result.proxy_revision);
        setDraft(draftFromPolicy(result));
        setError("保存已确认，并已回读当前策略。");
      } else
        setError(
          submission.pending
            ? "请求已受理，正在等待真实操作排空；202 不代表切换完成。"
            : "已回读服务端状态，草稿保留。请明确确认新版本后再保存，不会自动重发。",
        );
    } catch (failure) {
      if (!mounted.current || current !== sequence.current) return;
      const status = (failure as { status?: number })?.status;
      const prefix =
        status === 409
          ? "版本冲突或主体真实忙碌，草稿已保留"
          : submission.needsReadback
            ? "结果未知，仅允许刷新核对"
            : "操作失败";
      setError(`${prefix}：${proxyErrorText(failure)}。不会自动重发或切直连。`);
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
      const result = await submission.reconcile(() => adapter.policy(subject));
      if (!mounted.current || current !== sequence.current || !result) return;
      setPolicy(result);
      setPolicyError("");
      onUpdated?.();
      if (!switchPending(result.latest_switch)) submission.pending = null;
      setError("已回读当前策略。原草稿保留；核对后请明确确认使用当前版本。");
    } catch (failure) {
      if (mounted.current && current === sequence.current) {
        submission.needsReadback = true;
        setError(`回读失败：${proxyErrorText(failure)}。继续只读核对，不重发保存。`);
      }
    } finally {
      if (mounted.current && current === sequence.current) {
        setSaving(false);
        sync();
      }
    }
  }
  const pending = switchPending(policy?.latest_switch ?? null);
  const failoverBlocked =
    !policy ||
    policy.proxy_mode !== "group" ||
    !policy.agent_failover_enabled ||
    policy.remaining_switches <= 0 ||
    cooldownRemaining(policy.cooldown_until, now) > 0 ||
    pending ||
    policyRecoveryRequired(policy);
  const candidates = allowedCandidates(policy, groups);

  return (
    <Dialog open={open} onOpenChange={changeOpen}>
      <DialogTrigger asChild>
        <Button size="sm" variant="outline" aria-label={`切换${label}代理`}>
          <RadioTowerIcon data-icon="inline-start" />
          <span className="hidden lg:inline">出口策略</span>
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-2xl" showCloseButton={!saving}>
        <DialogHeader>
          <DialogTitle>{label}出口策略</DialogTitle>
          <DialogDescription>
            只保存明确选择；管理分组与使用授权分别生效。真实在途操作未退出时，服务端会拒绝切换。
          </DialogDescription>
        </DialogHeader>
        {loading && <p role="status">正在读取策略与代理目录…</p>}
        {policy && <ProxyPolicyStatus policy={policy} uncertain={readback || loading} />}
        {policy && (
          <ProxyPolicyRecovery
            subject={subject}
            adapter={adapter}
            disabled={!policyRecoveryRequired(policy) || saving || loading || readback || pending}
            onSaving={onSaving}
            onUpdated={(value) => {
              setPolicy(value);
              setPolicyError("");
              submission.needsReadback = false;
              submission.reviewRequired = true;
              if (!switchPending(value.latest_switch)) submission.pending = null;
              sync();
              onUpdated?.();
              setError("已核对人工恢复状态；原策略草稿保留，采用当前版本后才能再次保存。");
            }}
          />
        )}
        {draft && (
          <ProxyPolicyFields
            subject={subject}
            draft={draft}
            onChange={setDraft}
            groups={groups}
            proxies={proxies}
            disabled={saving || loading || readback || review || pending}
          />
        )}
        {policy?.proxy_mode === "group" && (
          <details className="rounded-lg border p-3 text-sm">
            <summary className="cursor-pointer font-medium">根据运行时证据请求故障切换</summary>
            <div className="mt-3 space-y-3">
              <p className="text-muted-foreground text-xs">
                只接受服务端记录的基础设施故障证据编号；输入原因不产生授权。受理后结束当前轮，等待操作排空，不重放有副作用工具。
              </p>
              <Label className="grid gap-2">
                故障原因
                <Input
                  maxLength={500}
                  value={reason}
                  onChange={(event) => setReason(event.target.value)}
                  disabled={saving || pending}
                />
              </Label>
              <Label className="grid gap-2">
                证据类型
                <select
                  className="h-10 rounded-md border bg-background px-3"
                  value={evidenceKind}
                  onChange={(event) => setEvidenceKind(event.target.value as typeof evidenceKind)}
                  disabled={saving || pending}
                >
                  <option value="connection_failure">连接故障</option>
                  <option value="tunnel_failure">隧道故障</option>
                  <option value="process_failure">进程故障</option>
                  <option value="health_confirmation">健康确认</option>
                </select>
              </Label>
              <Label className="grid gap-2">
                运行时证据编号
                <Input
                  value={reference}
                  onChange={(event) => setReference(event.target.value)}
                  disabled={saving || pending}
                />
              </Label>
              <Label className="grid gap-2">
                候选出口
                <select
                  className="h-10 rounded-md border bg-background px-3"
                  value={target}
                  onChange={(event) => setTarget(event.target.value)}
                  disabled={saving || pending}
                >
                  <option value="">由服务端在获准组内选择</option>
                  {candidates.map((member) => (
                    <option key={member.proxy_id} value={member.proxy_id}>
                      #{member.proxy_id} {member.proxy.name}
                    </option>
                  ))}
                </select>
              </Label>
              {failoverBlocked && (
                <p className="text-muted-foreground text-xs">
                  需已保存的分组授权、Agent 切换许可、剩余预算且冷却结束。待人工恢复时不可请求。
                </p>
              )}
              <Button
                variant="outline"
                disabled={
                  saving ||
                  loading ||
                  readback ||
                  review ||
                  !!blocked ||
                  failoverBlocked ||
                  !reason.trim() ||
                  !reference.trim() ||
                  (!!target && !candidates.some((member) => member.proxy_id === target))
                }
                onClick={() => void save(true)}
              >
                请求切换并等待排空
              </Button>
            </div>
          </details>
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
        {review && !readback && !pending && (
          <Button
            variant="outline"
            disabled={saving || loading || !policy}
            onClick={() => {
              if (policy && submission.confirmNewIntent()) {
                setExpected(policy.proxy_revision);
                sync();
                setError("已采用当前版本，草稿保持不变；再次保存才会修改。");
              }
            }}
          >
            核对完成，保留草稿并采用当前版本
          </Button>
        )}
        <DialogFooter>
          <Button variant="outline" disabled={saving} onClick={() => changeOpen(false)}>
            关闭
          </Button>
          <Button variant="outline" disabled={saving || loading} onClick={() => void reconcile()}>
            仅刷新核对
          </Button>
          <Button disabled={saving || loading || readback || review || !!blocked} onClick={() => void save()}>
            {saving ? "正在保存并核对…" : "保存出口策略"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
