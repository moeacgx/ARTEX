"use client";

import * as React from "react";

import { toast } from "sonner";

import { ProxyGroups } from "@/components/proxy-groups";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { WarpHealthControl } from "@/components/warp-health-control";
import { WarpHealthStatus } from "@/components/warp-health-status";
import { APIError, api } from "@/lib/api";
import {
  buildProxyPayload,
  proxyEndpoint as endpoint,
  proxyErrorText as errorText,
  type ProxyForm as Form,
  proxyFromFailure,
  proxySelectable as selectable,
  WARP_PHASE_LABEL,
  WARP_REGISTER_TERMS,
  WARP_TERMS_URL,
  warpActionNeedsTerms,
  warpAllowedActions,
  warpReady,
} from "@/lib/proxy-center";
import type { ProxyKind, ProxyRecord } from "@/lib/types";
import { WarpCreateIntent, type WarpCreateStage, type WarpIntent } from "@/lib/warp-create-intent";
import { warpCreateFailure } from "@/lib/warp-create-result";
import { warpEffectiveIP } from "@/lib/warp-health";

const KINDS: Array<{ value: ProxyKind; label: string }> = [
  { value: "http", label: "HTTP" },
  { value: "https", label: "HTTPS" },
  { value: "socks5", label: "SOCKS5" },
  { value: "warp", label: "轻量 WARP" },
  { value: "resin", label: "Resin 动态代理" },
];
const STATUS: Record<string, string> = {
  unknown: "未检测",
  healthy: "可用",
  failed: "失败",
  starting: "启动中",
  stopped: "已停止",
};
const freshForm = (): Form => ({
  name: "",
  kind: "http",
  protocol: "socks5",
  url: "",
  host: "",
  port: "",
  password: "",
  platform: "Default",
  adminURL: "",
  adminToken: "",
  enabled: true,
  clearPassword: false,
  clearAdminToken: false,
});
function badgeVariant(status: string): "default" | "secondary" | "destructive" {
  if (status === "failed") return "destructive";
  if (status === "healthy") return "default";
  return "secondary";
}

export function ProxyCenter() {
  const [items, setItems] = React.useState<ProxyRecord[]>([]);
  const [globalID, setGlobalID] = React.useState<number | null>(null);
  const [form, setForm] = React.useState<Form>(freshForm);
  const [editing, setEditing] = React.useState<ProxyRecord | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [loadError, setLoadError] = React.useState("");
  const [busy, setBusy] = React.useState("");
  const [deleting, setDeleting] = React.useState<ProxyRecord | null>(null);
  const [releasing, setReleasing] = React.useState<number | null>(null);
  const [releaseReadback, setReleaseReadback] = React.useState<number[]>([]);
  const releaseSubmitted = React.useRef(new Set<number>());
  const mutationLock = React.useRef(false);
  const [saveError, setSaveError] = React.useState("");
  const [warpCreation] = React.useState(() => new WarpCreateIntent());
  const [createIntent, setCreateIntent] = React.useState<WarpIntent | null>(null);
  const [createStage, setCreateStage] = React.useState<WarpCreateStage>("draft");
  const [intentStorageError, setIntentStorageError] = React.useState("正在读取本地 WARP 意图…");
  const [pendingID, setPendingID] = React.useState<number | null>(null);
  const [pollAttempt, setPollAttempt] = React.useState(0);
  const pollLock = React.useRef(false);
  const [registrationUncertain, setRegistrationUncertain] = React.useState<number[]>([]);
  // WARP 注册条款确认：registering 记录待确认的代理编号；acceptTerms 必须由用户
  // 手动勾选（每次打开都重置为 false，绝不预选）。
  const [registering, setRegistering] = React.useState<number | null>(null);
  const [acceptTerms, setAcceptTerms] = React.useState(false);

  const restoreIntent = React.useCallback(() => {
    try {
      const intent = warpCreation.restore();
      setCreateIntent(intent);
      setCreateStage(warpCreation.stage);
      setIntentStorageError("");
      if (intent) setForm({ ...freshForm(), kind: "warp", name: intent.name });
    } catch {
      setIntentStorageError("无法读取本地 WARP 意图，已暂停新增。请重试读取并核对原代理，勿重复新建。");
    }
  }, [warpCreation]);
  React.useEffect(restoreIntent, [restoreIntent]);

  const finishCreation = React.useCallback(
    (stage: WarpCreateStage) => {
      warpCreation.finish(stage);
      setCreateStage(stage);
      setCreateIntent(warpCreation.intent);
    },
    [warpCreation],
  );

  const newWarpIntent = () => {
    if (mutationLock.current) return;
    try {
      if (!warpCreation.newIntent()) return;
      setCreateIntent(null);
      setCreateStage("draft");
      setPendingID(null);
      setEditing(null);
      setForm({ ...freshForm(), kind: "warp" });
      setSaveError("");
      setIntentStorageError("");
    } catch {
      setIntentStorageError("无法清理原意图，已保留原请求编号。请重试读取本地存储。");
    }
  };

  const reload = React.useCallback(async () => {
    const result = await api.proxies();
    setItems(result.proxies ?? []);
    setGlobalID(result.global_proxy_id ?? null);
    setLoadError("");
  }, []);
  React.useEffect(() => {
    let active = true;
    api
      .proxies()
      .then((result) => {
        if (!active) return;
        setItems(result.proxies ?? []);
        setGlobalID(result.global_proxy_id ?? null);
      })
      .catch((error) => {
        if (active) setLoadError(errorText(error));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);
  React.useEffect(() => {
    if (createStage !== "pending" || !createIntent || pollAttempt === 0) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    let attempts = 0;
    const intent = createIntent;
    const poll = async () => {
      if (pollLock.current) {
        timer = setTimeout(() => void poll(), 2000);
        return;
      }
      pollLock.current = true;
      try {
        // 202 或结果未知后只 GET 原条目；运行健康不等同于创建意图已持久化完成。
        if (pendingID === null) {
          await reload();
          if (active) setSaveError("原创建请求尚无已确认编号。已刷新目录；不能按名称推断成功或再次注册。");
          return;
        }
        const item = await api.proxy(pendingID);
        if (!active || !warpCreation.isCurrent(intent)) return;
        if (pendingID !== null && item.id !== pendingID) throw new Error("返回的代理编号不一致");
        setItems((prev) => [item, ...prev.filter((entry) => entry.id !== item.id)]);
        if (warpReady(item)) {
          setSaveError(`WARP #${item.id} 当前已验证在线。原创建意图完成状态仍待服务端核对，不重发创建。`);
          return;
        }
        setSaveError(`WARP #${item.id} 已建档，当前未连接；请核对原条目，不会重新注册。`);
        if (++attempts < 30) timer = setTimeout(() => void poll(), 2000);
      } catch (error) {
        if (!active || !warpCreation.isCurrent(intent)) return;
        const result = warpCreateFailure(error);
        if (result.stage === "pending") {
          if (result.id !== null) setPendingID(result.id);
          const proxy = result.proxy;
          if (proxy) setItems((prev) => [proxy, ...prev.filter((item) => item.id !== proxy.id)]);
          if (++attempts < 30) timer = setTimeout(() => void poll(), 2000);
          else setSaveError("原请求仍在处理中，可继续核对进度；不会重新注册。");
        } else if (result.stage === "retained") {
          finishCreation("retained");
          setEditing(result.proxy);
          setSaveError(`WARP #${result.proxy.id} 已保留：${errorText(error)}。请恢复此条目。`);
        } else if (result.stage === "deleted" || result.stage === "conflict") {
          finishCreation(result.stage);
          setSaveError(`原请求已删除或冲突：${errorText(error)}。不会自动更换请求编号。`);
        } else setSaveError(`原请求状态暂时无法确认，可继续核对进度：${errorText(error)}`);
      } finally {
        pollLock.current = false;
      }
    };
    void poll();
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [createStage, pendingID, pollAttempt, finishCreation, warpCreation, createIntent, reload]);
  const reset = () => {
    if (mutationLock.current) return;
    setEditing(null);
    setForm(createIntent ? { ...freshForm(), kind: "warp", name: createIntent.name } : freshForm());
    setSaveError("");
  };
  const edit = (item: ProxyRecord) => {
    if (mutationLock.current) return;
    if (createStage === "pending" && pendingID === item.id) return;
    setEditing(item);
    setSaveError("");
    setForm({
      name: item.name,
      kind: item.kind,
      protocol: item.protocol === "http" ? "http" : "socks5",
      // 脱敏 URL 绝不能被当作真实凭据回写；留空让后端保留旧值。
      url: "",
      host: item.host ?? "",
      port: item.port ? String(item.port) : "",
      password: "",
      platform: item.resin_platform ?? "Default",
      adminURL: item.resin_admin_url ?? "",
      adminToken: "",
      enabled: item.enabled,
      clearPassword: false,
      clearAdminToken: false,
    });
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  async function refreshAfterMutation() {
    try {
      await reload();
    } catch (error) {
      setLoadError(errorText(error));
      toast.error(`操作已返回，但最新状态加载失败：${errorText(error)}。请刷新核对。`);
    }
  }

  async function save() {
    if (mutationLock.current) return;
    if (editing && createStage === "pending" && pendingID === editing.id) return;
    const creatingWarp = !editing && form.kind === "warp";
    let intent: WarpIntent | null = null;
    let data: ReturnType<typeof buildProxyPayload>;
    try {
      data = buildProxyPayload(form, !!editing);
      if (creatingWarp) {
        if (intentStorageError || createStage !== "draft") return;
        intent = warpCreation.begin(form.name, data.accept_terms === true);
        if (!intent) return;
        setCreateIntent(intent);
        data = { name: intent.name, kind: "warp", accept_terms: intent.accept_terms };
      }
    } catch (error) {
      if (creatingWarp && !warpCreation.storageReady) setIntentStorageError(errorText(error));
      toast.error(errorText(error));
      return;
    }
    mutationLock.current = true;
    setBusy("save");
    setSaveError("");
    try {
      const result = editing ? await api.updateProxy(editing.id, data) : await api.createProxy(data, intent?.key);
      if (intent && !warpCreation.isCurrent(intent)) return;
      if (creatingWarp && !warpReady(result)) {
        finishCreation("retained");
        setEditing(result);
        setSaveError(`WARP #${result.id} 已建档，但尚未连接成功。请核对状态并恢复该条目。`);
      } else {
        let message = "代理已加入中心";
        if (editing) message = "代理配置已保存";
        else if (form.kind === "warp") message = `WARP 已连接 · 出口 ${warpEffectiveIP(result) ?? "未验证"}`;
        toast.success(message);
        if (creatingWarp) {
          finishCreation("complete");
          setPendingID(result.id);
          setForm({ ...freshForm(), kind: "warp", name: data.name });
        } else setForm(freshForm());
        setEditing(null);
      }
      await refreshAfterMutation();
    } catch (error) {
      if (intent && !warpCreation.isCurrent(intent)) return;
      const retained = proxyFromFailure(error);
      const status = error instanceof APIError ? error.status : undefined;
      const failure = warpCreateFailure(error);
      if (creatingWarp && failure.stage === "pending") {
        finishCreation("pending");
        setPendingID(failure.id);
        setPollAttempt((value) => value + 1);
        setSaveError(`WARP ${failure.id ? `#${failure.id} ` : ""}正在添加 / 连接，正在核对原请求。`);
      } else if (creatingWarp && (status === 409 || status === 410)) {
        finishCreation(status === 410 ? "deleted" : "conflict");
        setSaveError(
          status === 410
            ? "原请求对应的代理已删除，此请求编号不能再次创建。需要新出口时请选择新增另一个。"
            : "原请求与服务端记录冲突，请先核对列表。不会自动更换请求编号重新创建。",
        );
      } else if (retained) {
        if (creatingWarp) finishCreation("retained");
        setEditing(retained);
        setItems((prev) => [retained, ...prev.filter((item) => item.id !== retained.id)]);
        setSaveError(`WARP #${retained.id} 已保留：${errorText(error)}。请恢复此条目，勿重复添加。`);
      } else {
        if (creatingWarp) finishCreation("uncertain");
        setSaveError(
          creatingWarp
            ? `添加结果待确认：${errorText(error)}。已保留原请求编号，只刷新核对，不重发创建或注册。`
            : `保存失败：${errorText(error)}`,
        );
      }
      if (status !== 202) toast.error(errorText(error));
    } finally {
      mutationLock.current = false;
      setBusy("");
    }
  }
  async function choose(id: number | null) {
    setBusy("global");
    try {
      await api.setGlobalProxy(id);
      toast.success(id === null ? "全局默认已设为直连" : `全局默认已设为代理 #${id}`);
      await refreshAfterMutation();
    } catch (error) {
      toast.error(`设置失败：${errorText(error)}`);
    } finally {
      setBusy("");
    }
  }
  async function test(id: number) {
    setBusy(`test:${id}`);
    try {
      const result = await api.testProxy(id);
      await refreshAfterMutation();
      if (result.status === "healthy") toast.success(`代理 #${id} 检测可用`);
      else toast.info(`代理 #${id} 检测完成：${STATUS[result.status] ?? result.status}`);
    } catch (error) {
      try {
        await reload();
      } catch {
        /* 保留检测错误 */
      }
      toast.error(`代理 #${id} 检测失败：${errorText(error)}`);
    } finally {
      setBusy("");
    }
  }
  // 注册必须由用户先确认条款：只有确认后才会带着 accept_terms:true 调用；
  // start/stop/recover 不涉及条款，也不带请求体。
  async function warp(id: number, action: "register" | "start" | "stop" | "recover") {
    if (mutationLock.current) return;
    if (createStage === "pending" && pendingID === id) return;
    const current = items.find((item) => item.id === id) ?? (editing?.id === id ? editing : null);
    if (
      !current ||
      !warpAllowedActions(current).includes(action) ||
      (action === "register" && registrationUncertain.includes(id))
    )
      return;
    if (warpActionNeedsTerms(action) && acceptTerms !== true) return;
    mutationLock.current = true;
    if (action === "register") setRegistrationUncertain((prev) => [...prev, id]);
    setBusy(`warp:${id}`);
    try {
      const result = await api.proxyWarpAction(id, action, warpActionNeedsTerms(action) ? true : undefined);
      setItems((prev) => prev.map((item) => (item.id === id ? result : item)));
      if (editing?.id === id) setEditing(result);
      await refreshAfterMutation();
      if (warpReady(result)) {
        toast.success(`WARP #${id} 已连接 · 出口 ${warpEffectiveIP(result) ?? "未验证"}`);
        if (editing?.id === id) setSaveError("");
      } else toast.info(`WARP #${id} 当前状态：${WARP_PHASE_LABEL[result.warp_phase ?? ""] ?? "状态待核对"}`);
      setRegistering(null);
      setAcceptTerms(false);
    } catch (error) {
      const retained = proxyFromFailure(error);
      if (retained) {
        setItems((prev) => prev.map((item) => (item.id === id ? retained : item)));
        if (editing?.id === id) setEditing(retained);
      }
      try {
        await reload();
      } catch {
        /* 保留操作错误 */
      }
      toast.error(`WARP #${id} 操作失败：${errorText(error)}`);
      // 注册失败或响应丢失均不重注册；后续仅按服务端 phase 恢复原 ID。
      setRegistering(null);
      setAcceptTerms(false);
    } finally {
      mutationLock.current = false;
      setBusy("");
    }
  }
  async function release(id: number) {
    if (mutationLock.current) return;
    mutationLock.current = true;
    setBusy(`resin:${id}`);
    try {
      if (!releaseSubmitted.current.has(id)) {
        releaseSubmitted.current.add(id);
        setReleaseReadback((previous) => [...previous, id]);
        await api.releaseResin(id);
      }
      await refreshAfterMutation();
      toast.info(`Resin #${id} 原请求状态已刷新，请核对运行状态；不会重复释放`);
      setReleasing(null);
    } catch (error) {
      toast.error(`Resin #${id} 释放待核对：${errorText(error)}。仅刷新，不重发释放请求。`);
    } finally {
      mutationLock.current = false;
      setBusy("");
    }
  }
  async function remove(item: ProxyRecord) {
    if (mutationLock.current) return;
    mutationLock.current = true;
    setBusy(`delete:${item.id}`);
    try {
      await api.deleteProxy(item.id);
      mutationLock.current = false;
      if (editing?.id === item.id) reset();
      setDeleting(null);
      toast.success(`代理 #${item.id} 已删除`);
      await refreshAfterMutation();
    } catch (error) {
      toast.error(`删除失败：${errorText(error)}`);
    } finally {
      mutationLock.current = false;
      setBusy("");
    }
  }

  const global = items.find((item) => item.id === globalID);
  const creatingWarp = !editing && form.kind === "warp";
  const creationBlocked = creatingWarp && (!!intentStorageError || createStage !== "draft");
  let saveLabel = "添加代理";
  if (!editing && form.kind === "warp") saveLabel = "确认添加（同意条款）";
  if (creatingWarp && createStage === "uncertain") saveLabel = "原请求结果待核对";
  if (creatingWarp && createStage === "complete") saveLabel = "已完成添加";
  if (creatingWarp && createStage === "pending") saveLabel = "正在添加 / 连接…";
  if (creatingWarp && createStage === "deleted") saveLabel = "原代理已删除";
  if (creatingWarp && createStage === "conflict") saveLabel = "请核对原请求";
  if (creatingWarp && createStage === "retained") saveLabel = "已保留原代理";
  if (editing) saveLabel = "保存修改";
  if (busy === "save") saveLabel = "保存中…";
  if (busy === "save" && form.kind === "warp" && !editing) saveLabel = "正在添加 / 连接…";
  let globalLabel = "直连";
  if (globalID) globalLabel = `#${globalID}（记录未返回）`;
  if (global) globalLabel = `#${global.id} ${global.name}`;
  if (loading || loadError) globalLabel = "正在核对或状态未知";
  return (
    <div className="flex flex-1 flex-col gap-5">
      <div>
        <h1 className="font-semibold text-xl tracking-tight">代理中心</h1>
        <p className="text-muted-foreground text-sm">
          普通代理、轻量 WARP 与 Resin 共享编号。任务与对话可明确授权使用分组，候选耗尽不会回落直连。
        </p>
      </div>
      <ProxyGroups proxies={items} />
      <Card>
        <CardHeader>
          <CardTitle>{editing ? `编辑代理 #${editing.id}` : "新增代理"}</CardTitle>
          <CardDescription>
            {form.kind === "warp"
              ? "输入名称并确认添加，系统自动注册、启动和验证出口。"
              : "凭据不回显；编辑时秘密字段留空表示保留，勾选清除才会删除。"}
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <div className="grid gap-2">
            <Label htmlFor="proxy-name">名称</Label>
            <Input
              id="proxy-name"
              disabled={!!busy || (creatingWarp && !!createIntent)}
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="proxy-kind">类型</Label>
            <select
              id="proxy-kind"
              className="h-10 rounded-md border border-input bg-background px-3 text-sm"
              value={form.kind}
              disabled={!!editing || !!busy || (creatingWarp && !!createIntent)}
              onChange={(e) => setForm({ ...form, kind: e.target.value as ProxyKind })}
            >
              {KINDS.map((kind) => (
                <option key={kind.value} value={kind.value}>
                  {kind.label}
                </option>
              ))}
            </select>
          </div>
          {form.kind !== "warp" && form.kind !== "resin" && (
            <div className="grid gap-2 md:col-span-2">
              <Label htmlFor="proxy-url">代理 URL {editing ? "（留空保留原地址）" : ""}</Label>
              {editing && <p className="break-all text-muted-foreground text-xs">当前：{endpoint(editing)}</p>}
              <Input
                id="proxy-url"
                autoComplete="off"
                value={form.url}
                onChange={(e) => setForm({ ...form, url: e.target.value })}
                placeholder={`${form.kind}://user@host:port`}
              />
              <Label htmlFor="ordinary-password">认证密码 {editing?.has_password ? "（已保存）" : ""}</Label>
              <Input
                id="ordinary-password"
                type="password"
                autoComplete="new-password"
                value={form.password}
                disabled={form.clearPassword}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                placeholder={editing ? "留空保留原密码" : "可选，也可包含在 URL 中"}
              />
              {editing && (
                <label className="flex gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={form.clearPassword}
                    onChange={(e) => setForm({ ...form, clearPassword: e.target.checked, password: "" })}
                  />
                  明确清除认证密码
                </label>
              )}
            </div>
          )}
          {form.kind === "warp" && (
            <div className="space-y-2 text-muted-foreground text-sm md:col-span-2">
              {!editing && (
                <p>
                  点击“确认添加”表示同意{" "}
                  <a href={WARP_TERMS_URL} target="_blank" rel="noreferrer" className="underline">
                    Cloudflare WARP 服务条款
                  </a>
                  ，并申请注册。轻量 WARP 提供本机 SOCKS5 出口。
                </p>
              )}
              {creatingWarp && createStage === "uncertain" && (
                <p>已保留「{createIntent?.name}」的原 UUID 添加意图。结果未知时仅可核对列表，不再次创建或注册。</p>
              )}
              {creatingWarp && createStage === "complete" && (
                <p role="status">WARP #{pendingID} 已完成添加。需要另一个出口时，请点击“新增另一个”。</p>
              )}
              {intentStorageError && (
                <div role="alert" className="space-y-2 text-destructive">
                  <p>{intentStorageError}</p>
                  <Button variant="outline" disabled={!!busy} onClick={restoreIntent}>
                    重试读取本地意图
                  </Button>
                </div>
              )}
              {editing && (
                <p>
                  代理 #{editing.id} · {WARP_PHASE_LABEL[editing.warp_phase ?? ""] ?? "状态待核对"} · 出口 IP：
                  {warpEffectiveIP(editing) ?? "未验证"}
                </p>
              )}
            </div>
          )}
          {form.kind === "resin" && (
            <>
              <div className="grid gap-2">
                <Label htmlFor="proxy-host">数据面 Host</Label>
                <Input id="proxy-host" value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="proxy-port">数据面 Port</Label>
                <Input
                  id="proxy-port"
                  inputMode="numeric"
                  value={form.port}
                  onChange={(e) => setForm({ ...form, port: e.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="proxy-protocol">数据面协议</Label>
                <select
                  id="proxy-protocol"
                  className="h-10 rounded-md border border-input bg-background px-3 text-sm"
                  value={form.protocol}
                  onChange={(e) => setForm({ ...form, protocol: e.target.value as Form["protocol"] })}
                >
                  <option value="socks5">SOCKS5</option>
                  <option value="http">HTTP</option>
                </select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="resin-platform">Platform</Label>
                <Input
                  id="resin-platform"
                  value={form.platform}
                  onChange={(e) => setForm({ ...form, platform: e.target.value })}
                />
                <p className="text-muted-foreground text-xs">任务 Account 按任务编号生成，不在此设置。</p>
              </div>
              <div className="grid gap-2 md:col-span-2">
                <Label htmlFor="resin-admin-url">Admin URL（可选）</Label>
                <Input
                  id="resin-admin-url"
                  value={form.adminURL}
                  onChange={(e) => setForm({ ...form, adminURL: e.target.value })}
                  placeholder="https://resin.example.com"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="proxy-password">Proxy Token {editing?.has_password ? "（已保存）" : ""}</Label>
                <Input
                  id="proxy-password"
                  type="password"
                  autoComplete="new-password"
                  value={form.password}
                  disabled={form.clearPassword}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  placeholder={editing ? "留空保留" : "输入 Token"}
                />
                {editing && (
                  <label className="flex gap-2 text-xs">
                    <input
                      type="checkbox"
                      checked={form.clearPassword}
                      onChange={(e) => setForm({ ...form, clearPassword: e.target.checked, password: "" })}
                    />
                    明确清除 Proxy Token
                  </label>
                )}
              </div>
              <div className="grid gap-2">
                <Label htmlFor="resin-admin-token">
                  Admin Token {editing?.has_resin_admin_token ? "（已保存）" : ""}
                </Label>
                <Input
                  id="resin-admin-token"
                  type="password"
                  autoComplete="new-password"
                  value={form.adminToken}
                  disabled={form.clearAdminToken}
                  onChange={(e) => setForm({ ...form, adminToken: e.target.value })}
                  placeholder="留空保留（新建可选）"
                />
                {editing && (
                  <label className="flex gap-2 text-xs">
                    <input
                      type="checkbox"
                      checked={form.clearAdminToken}
                      onChange={(e) => setForm({ ...form, clearAdminToken: e.target.checked, adminToken: "" })}
                    />
                    明确清除 Admin Token
                  </label>
                )}
              </div>
            </>
          )}
          {(form.kind !== "warp" || editing) && (
            <div className="flex items-center gap-3 md:col-span-2">
              <Switch
                id="proxy-enabled"
                checked={form.enabled}
                onCheckedChange={(enabled) => setForm({ ...form, enabled })}
              />
              <Label htmlFor="proxy-enabled">启用此代理</Label>
            </div>
          )}
          {saveError && (
            <p role="alert" className="text-destructive text-sm md:col-span-2">
              {saveError}
            </p>
          )}
          <div className="flex gap-2 md:col-span-2">
            <Button disabled={!!busy || creationBlocked} onClick={() => void save()}>
              {saveLabel}
            </Button>
            {form.kind === "warp" && createIntent && (
              <Button
                variant="outline"
                disabled={!!busy || createStage === "pending" || createStage === "uncertain" || !!intentStorageError}
                onClick={newWarpIntent}
              >
                新增另一个
              </Button>
            )}
            {creatingWarp && createStage === "pending" && (
              <Button variant="outline" disabled={!!busy} onClick={() => setPollAttempt((value) => value + 1)}>
                核对进度 {pendingID ? `#${pendingID}` : ""}
              </Button>
            )}
            {editing && (
              <Button variant="outline" disabled={!!busy} onClick={reset}>
                取消编辑
              </Button>
            )}
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>代理列表</CardTitle>
          <CardDescription>
            全局默认：{globalLabel}
            。更改仅影响之后创建的任务快照。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex gap-2">
            <Button
              variant={globalID === null ? "default" : "outline"}
              disabled={!!busy || loading || !!loadError || globalID === null}
              onClick={() => void choose(null)}
            >
              全局默认直连
            </Button>
            <Button
              variant="outline"
              disabled={!!busy || loading}
              onClick={() => {
                setLoading(true);
                void reload()
                  .catch((error) => setLoadError(errorText(error)))
                  .finally(() => setLoading(false));
              }}
            >
              刷新状态
            </Button>
          </div>
          {loading && <p className="text-muted-foreground text-sm">正在加载代理…</p>}
          {!loading && loadError && (
            <p role="alert" className="text-destructive text-sm">
              代理列表加载失败：{loadError}
            </p>
          )}
          {!loading && !loadError && items.length === 0 && <p className="text-muted-foreground text-sm">暂无代理。</p>}
          {!loading &&
            !loadError &&
            items.map((item) => (
              <div key={item.id} className="space-y-2 rounded-lg border p-3">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <strong>
                        #{item.id} {item.name}
                      </strong>
                      <Badge variant={badgeVariant(item.status)}>{STATUS[item.status] ?? item.status}</Badge>
                      {!item.enabled && <Badge variant="outline">已停用</Badge>}
                      {globalID === item.id && <Badge variant="outline">全局默认</Badge>}
                    </div>
                    <p className="text-muted-foreground text-xs">
                      {KINDS.find((kind) => kind.value === item.kind)?.label ?? item.kind} ·{" "}
                      <span className="break-all font-mono">{endpoint(item)}</span>
                    </p>
                    {item.kind === "resin" && (
                      <p className="text-muted-foreground text-xs">
                        Platform：{item.resin_platform || "Default"} · Proxy Token：
                        {item.has_password ? "已保存" : "未确认"} · Admin Token：
                        {item.has_resin_admin_token ? "已保存" : "未确认"}
                      </p>
                    )}
                    {item.last_checked_at && (
                      <p className="text-muted-foreground text-xs">
                        最近检测：{new Date(item.last_checked_at).toLocaleString()}
                      </p>
                    )}
                    {item.last_error && (
                      <p className="break-all text-destructive text-xs">诊断：{errorText(item.last_error)}</p>
                    )}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      size="sm"
                      variant={globalID === item.id ? "default" : "outline"}
                      disabled={!!busy || !selectable(item) || globalID === item.id}
                      onClick={() => void choose(item.id)}
                    >
                      {globalID === item.id ? "全局默认" : "设为默认"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={!!busy || (createStage === "pending" && pendingID === item.id)}
                      onClick={() => edit(item)}
                    >
                      编辑
                    </Button>
                    <Button size="sm" variant="outline" disabled={!!busy} onClick={() => void test(item.id)}>
                      {busy === `test:${item.id}` ? "检测中…" : "检测"}
                    </Button>
                    <Button size="sm" variant="destructive" disabled={!!busy} onClick={() => setDeleting(item)}>
                      删除
                    </Button>
                  </div>
                </div>
                {item.kind === "warp" && (
                  <div className="flex flex-wrap gap-2 border-t pt-2">
                    <WarpHealthStatus proxy={item} />
                    <WarpHealthControl
                      key={item.id}
                      proxy={item}
                      onUpdated={(value) =>
                        setItems((previous) => previous.map((entry) => (entry.id === value.id ? value : entry)))
                      }
                    />
                    <p className="w-full text-muted-foreground text-xs">
                      {WARP_PHASE_LABEL[item.warp_phase ?? ""] ?? "状态待核对"} · 出口 IP：
                      {warpEffectiveIP(item) ?? "未验证"}
                    </p>
                    {warpAllowedActions(item)
                      .filter((action) => action !== "recover")
                      .map((action) => (
                        <Button
                          key={action}
                          size="sm"
                          variant="outline"
                          disabled={
                            !!busy ||
                            (createStage === "pending" && pendingID === item.id) ||
                            (action === "register" && registrationUncertain.includes(item.id)) ||
                            (action === "start" && item.status === "healthy") ||
                            (action === "stop" && item.status === "stopped")
                          }
                          onClick={() => {
                            if (warpActionNeedsTerms(action)) {
                              // 注册：先弹确认框，用户明确同意后才发请求。
                              setAcceptTerms(false);
                              setRegistering(item.id);
                              return;
                            }
                            void warp(item.id, action);
                          }}
                        >
                          {{ register: "注册", start: "启动", stop: "停止", recover: "恢复" }[action]}
                        </Button>
                      ))}
                    {(item.warp_phase === "registering" || registrationUncertain.includes(item.id)) && (
                      <p className="text-muted-foreground text-xs">
                        注册结果可能不确定，请核对原条目；不会自动重复注册。
                      </p>
                    )}
                  </div>
                )}
                {item.kind === "resin" && (
                  <div className="border-t pt-2">
                    <Button size="sm" variant="outline" disabled={!!busy} onClick={() => setReleasing(item.id)}>
                      {releaseReadback.includes(item.id) ? "核对租约状态" : "释放租约"}
                    </Button>
                  </div>
                )}
              </div>
            ))}
        </CardContent>
      </Card>

      <AlertDialog
        open={deleting !== null}
        onOpenChange={(open) => {
          if (!open && !mutationLock.current) setDeleting(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>删除代理「{deleting?.name}」？</AlertDialogTitle>
            <AlertDialogDescription>
              {deleting?.kind === "warp"
                ? "删除会自动断开 WARP 出口并清理本地注册档案。"
                : "删除后此代理将从代理中心移除。"}
              正在创建、被任务或渠道引用、设为全局默认的代理无法删除，请先解除引用。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={!!busy}>取消</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={!!busy}
              onClick={(event) => {
                event.preventDefault();
                if (deleting) void remove(deleting);
              }}
            >
              {busy.startsWith("delete:") ? "删除中…" : "确认删除"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <Dialog
        open={releasing !== null}
        onOpenChange={(open) => {
          if (!open && !mutationLock.current) setReleasing(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>释放 Resin #{releasing} 的租约？</DialogTitle>
            <DialogDescription>
              仅请求释放此出口的租约，服务端会检查实际使用状态；不会强制切换任务到直连。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" disabled={!!busy} onClick={() => setReleasing(null)}>
              取消
            </Button>
            <Button
              disabled={!!busy}
              onClick={() => {
                if (releasing !== null) void release(releasing);
              }}
            >
              {releasing !== null && releaseReadback.includes(releasing) ? "仅刷新核对" : "确认请求释放"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* WARP 注册条款确认：勾选项默认不选中；取消不发请求；确认后才带 accept_terms:true。 */}
      <Dialog
        open={registering !== null}
        onOpenChange={(open) => {
          if (mutationLock.current) return;
          if (!open) {
            setRegistering(null);
            setAcceptTerms(false);
          }
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>注册 WARP 出口 #{registering}</DialogTitle>
            <DialogDescription>注册前请阅读并确认以下说明；未确认不会发起任何注册请求。</DialogDescription>
          </DialogHeader>
          <ul className="list-disc space-y-2 pl-5 text-muted-foreground text-sm">
            {WARP_REGISTER_TERMS.map((line) => (
              <li key={line}>{line}</li>
            ))}
          </ul>
          <a href={WARP_TERMS_URL} target="_blank" rel="noreferrer" className="text-sm underline">
            Cloudflare WARP 官方服务条款
          </a>
          <Label htmlFor="warp-accept-terms" className="flex items-start gap-2 font-normal text-sm">
            <Checkbox
              id="warp-accept-terms"
              checked={acceptTerms}
              onCheckedChange={(checked) => setAcceptTerms(checked === true)}
            />
            <span>我已阅读并同意上述注册说明，确认现在向 Cloudflare 申请注册。</span>
          </Label>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={!!busy}
              onClick={() => {
                setRegistering(null);
                setAcceptTerms(false);
              }}
            >
              取消
            </Button>
            <Button
              type="button"
              disabled={!acceptTerms || !!busy}
              onClick={() => {
                if (registering === null) return;
                void warp(registering, "register");
              }}
            >
              {busy ? "注册中…" : "同意并注册"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
