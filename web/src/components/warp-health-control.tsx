"use client";

import * as React from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { proxyErrorText } from "@/lib/proxy-center";
import { RevisionSubmission } from "@/lib/proxy-policy";
import type { ProxyRecord } from "@/lib/types";
import type { WarpHealthPolicy } from "@/lib/warp-health";
import { type WarpHealthAdapter, warpHealthAPI } from "@/lib/warp-health-api";

export function WarpHealthControl({
  proxy,
  onUpdated,
  adapter = warpHealthAPI,
}: {
  proxy: ProxyRecord;
  onUpdated: (proxy: ProxyRecord) => void;
  adapter?: WarpHealthAdapter;
}) {
  const [open, setOpen] = React.useState(false);
  const [recoverOpen, setRecoverOpen] = React.useState(false);
  const [policy, setPolicy] = React.useState<WarpHealthPolicy | null>(null);
  const [draft, setDraft] = React.useState<boolean | null>(null);
  const [expected, setExpected] = React.useState<number | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState("");
  const [review, setReview] = React.useState(false);
  const [readback, setReadback] = React.useState(false);
  const [recoveryReadback, setRecoveryReadback] = React.useState(false);
  const [submission] = React.useState(() => new RevisionSubmission<WarpHealthPolicy>());
  const recoveryLock = React.useRef(false);
  const recoverySubmitted = React.useRef(false);
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
  async function load(initialize: boolean) {
    const current = ++sequence.current;
    setLoading(true);
    try {
      const value = await adapter.policy(Number(proxy.id));
      if (!mounted.current || current !== sequence.current) return;
      setPolicy(value);
      if (initialize && !submission.reviewRequired && !submission.needsReadback) {
        setDraft(value.auto_recover_enabled);
        setExpected(value.revision);
      }
      setError("");
    } catch (failure) {
      if (mounted.current && current === sequence.current) {
        setPolicy(null);
        setError(`健康策略读取失败：${proxyErrorText(failure)}。开关保持未知，不用默认值代替。`);
      }
    } finally {
      if (mounted.current && current === sequence.current) setLoading(false);
    }
  }
  function changeOpen(value: boolean) {
    if (submission.busy || recoveryLock.current) return;
    setOpen(value);
    if (value) void load(true);
    else {
      sequence.current++;
      setLoading(false);
    }
  }
  async function save() {
    if (submission.busy || expected === null || draft === null || readback || review || !policy) return;
    const current = sequence.current;
    setSaving(true);
    setError("");
    try {
      const value = await submission.submit(
        async () => {
          await adapter.save(Number(proxy.id), expected, draft);
        },
        () => adapter.policy(Number(proxy.id)),
        (result) => result.revision > expected && result.auto_recover_enabled === draft,
      );
      if (!mounted.current || current !== sequence.current || !value) return;
      setPolicy(value);
      if (submission.confirmed) {
        setExpected(value.revision);
        setDraft(value.auto_recover_enabled);
        setError("健康策略已保存并回读确认。");
      } else setError("已回读当前健康策略，草稿保留；请明确采用新版本后保存。");
    } catch (failure) {
      if (mounted.current && current === sequence.current)
        setError(`保存未确认：${proxyErrorText(failure)}。草稿保留，只允许 GET 核对，不自动重发。`);
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
      const value = await submission.reconcile(() => adapter.policy(Number(proxy.id)));
      if (!mounted.current || current !== sequence.current || !value) return;
      setPolicy(value);
      setError(`已核对当前健康策略版本 ${value.revision}。草稿保留，请明确采用当前版本。`);
    } catch (failure) {
      if (mounted.current && current === sequence.current) {
        submission.needsReadback = true;
        setError(`核对失败：${proxyErrorText(failure)}。不会重发 PUT。`);
      }
    } finally {
      if (mounted.current && current === sequence.current) {
        setSaving(false);
        sync();
      }
    }
  }
  async function recover() {
    if (recoveryLock.current) return;
    recoveryLock.current = true;
    setSaving(true);
    setError("");
    const current = sequence.current;
    try {
      if (!recoverySubmitted.current) {
        // 提交前即封住重发；断网与迟到结果均只回读原档案。
        recoverySubmitted.current = true;
        setRecoveryReadback(true);
        await adapter.recover(Number(proxy.id));
      }
      const value = await adapter.proxy(Number(proxy.id));
      if (!mounted.current || current !== sequence.current) return;
      if (Number(value.id) !== Number(proxy.id)) throw new Error("恢复回读编号不一致");
      onUpdated(value);
      setError("已回读原出口运行状态。若仍待人工恢复，请核对服务端退出与空闲检查；不会重复提交恢复。");
    } catch (failure) {
      if (mounted.current && current === sequence.current)
        setError(`恢复状态待核对：${proxyErrorText(failure)}。不会重新注册、强制直连或自行解除屏障。`);
    } finally {
      recoveryLock.current = false;
      if (mounted.current && current === sequence.current) setSaving(false);
    }
  }
  return (
    <>
      <Button size="sm" variant="outline" onClick={() => changeOpen(true)}>
        自动恢复设置
      </Button>
      <Button
        size="sm"
        variant="outline"
        disabled={saving}
        onClick={() => {
          setError("");
          setRecoverOpen(true);
        }}
      >
        {recoveryReadback ? "核对原恢复结果" : "人工核对与恢复"}
      </Button>
      <Dialog open={open} onOpenChange={changeOpen}>
        <DialogContent showCloseButton={!saving}>
          <DialogHeader>
            <DialogTitle>WARP #{proxy.id} 自动恢复</DialogTitle>
            <DialogDescription>
              自动恢复开关与运行意图独立。关闭仍持续检测；运行意图为关闭时不会自动启动。
            </DialogDescription>
          </DialogHeader>
          {loading && <p role="status">正在读取权威健康策略…</p>}
          {policy && (
            <>
              <p className="text-xs">
                当前版本 {policy.revision} · 已保存：{policy.auto_recover_enabled ? "允许自动恢复" : "仅持续探测"}
              </p>
              <div className="flex items-center justify-between gap-3">
                <Label htmlFor={`warp-${proxy.id}-automatic`}>允许后台安全自动恢复</Label>
                <Switch
                  id={`warp-${proxy.id}-automatic`}
                  checked={draft === true}
                  disabled={loading || saving || readback || review || draft === null}
                  onCheckedChange={setDraft}
                />
              </div>
              <p className="text-muted-foreground text-xs">
                每 {policy.probe_interval_seconds} 秒检测，连续 {policy.failure_threshold} 次失败后复探。恢复失败按{" "}
                {policy.recovery_min_seconds}–{policy.recovery_max_seconds} 秒退避，复用原注册材料，可能保留相同 IP。
              </p>
            </>
          )}
          {error && (
            <p role="status" className="text-sm">
              {error}
            </p>
          )}
          {review && !readback && policy && (
            <Button
              variant="outline"
              disabled={saving}
              onClick={() => {
                if (submission.confirmNewIntent()) {
                  setExpected(policy.revision);
                  sync();
                  setError("已采用当前版本，原开关草稿保留；再次保存才生效。");
                }
              }}
            >
              核对完成并采用当前版本
            </Button>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={saving} onClick={() => changeOpen(false)}>
              关闭
            </Button>
            <Button variant="outline" disabled={saving || loading} onClick={() => void reconcile()}>
              仅刷新核对
            </Button>
            <Button
              disabled={loading || saving || !policy || readback || review || draft === null}
              onClick={() => void save()}
            >
              保存自动恢复设置
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog
        open={recoverOpen}
        onOpenChange={(value) => {
          if (!recoveryLock.current) {
            setRecoverOpen(value);
            if (!value) sequence.current++;
          }
        }}
      >
        <DialogContent showCloseButton={!saving}>
          <DialogHeader>
            <DialogTitle>核对并恢复原 WARP #{proxy.id}</DialogTitle>
            <DialogDescription>
              提交恢复意图后，由服务端证明旧执行与进程退出、资源空闲，再 Stop/Wait
              并恢复。前端不提交退出自证，也不解除屏障。
            </DialogDescription>
          </DialogHeader>
          <p className="text-muted-foreground text-xs">
            恢复复用原注册材料，不重新注册或新建出口，不保证换成不同 IP。取消不发请求。
          </p>
          {error && <p role="status">{error}</p>}
          <DialogFooter>
            <Button variant="outline" disabled={saving} onClick={() => setRecoverOpen(false)}>
              取消
            </Button>
            <Button disabled={saving} onClick={() => void recover()}>
              {recoveryReadback ? "仅回读原出口状态" : "提交人工恢复意图"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
