"use client";

import * as React from "react";

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
import { proxyErrorText } from "@/lib/proxy-center";
import { type ProxyGroup, proxyUnavailable, RevisionSubmission, validID, validRevision } from "@/lib/proxy-policy";
import { type ProxyPolicyAdapter, proxyPolicyAPI } from "@/lib/proxy-policy-api";
import type { ProxyRecord } from "@/lib/types";

type Editor = { group: ProxyGroup | null; name: string; enabled: boolean; ids: string[]; expected: number | null };
export function ProxyGroups({
  proxies,
  adapter = proxyPolicyAPI,
}: {
  proxies: ProxyRecord[];
  adapter?: ProxyPolicyAdapter;
}) {
  const [groups, setGroups] = React.useState<ProxyGroup[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [listError, setListError] = React.useState("");
  const [editor, setEditor] = React.useState<Editor | null>(null);
  const [open, setOpen] = React.useState(false);
  const [search, setSearch] = React.useState("");
  const [message, setMessage] = React.useState("");
  const [saving, setSaving] = React.useState(false);
  const [review, setReview] = React.useState(false);
  const [readback, setReadback] = React.useState(false);
  const [createUnknown, setCreateUnknown] = React.useState(false);
  const [deleting, setDeleting] = React.useState<ProxyGroup | null>(null);
  const [deleteUnknown, setDeleteUnknown] = React.useState(false);
  const lock = React.useRef(false);
  const sequence = React.useRef(0);
  const mounted = React.useRef(true);
  const submission = React.useRef(new RevisionSubmission<ProxyGroup>());
  React.useEffect(() => {
    mounted.current = true;
    const current = ++sequence.current;
    void adapter
      .groups()
      .then((value) => {
        if (mounted.current && current === sequence.current) setGroups(value);
      })
      .catch((failure: unknown) => {
        if (mounted.current) setListError(proxyErrorText(failure));
      })
      .finally(() => {
        if (mounted.current) setLoading(false);
      });
    return () => {
      mounted.current = false;
      sequence.current++;
    };
  }, [adapter]);
  const sync = () => {
    setReview(submission.current.reviewRequired);
    setReadback(submission.current.needsReadback);
  };
  async function refresh() {
    if (lock.current) return;
    setLoading(true);
    try {
      const value = await adapter.groups();
      if (mounted.current) {
        setGroups(value);
        setListError("");
      }
    } catch (failure) {
      if (mounted.current) setListError(proxyErrorText(failure));
    } finally {
      if (mounted.current) setLoading(false);
    }
  }
  function begin(group: ProxyGroup | null) {
    if (lock.current) return;
    // 未知创建结果不能通过打开新表单隐式再次 POST。
    if (createUnknown && !group) {
      setOpen(true);
      return;
    }
    if (group && editor?.group?.id === group.id && (readback || review)) {
      setOpen(true);
      return;
    }
    if (group) setCreateUnknown(false);
    sequence.current++;
    submission.current = new RevisionSubmission<ProxyGroup>();
    setEditor({
      group,
      name: group?.name ?? "",
      enabled: group?.enabled ?? true,
      ids: group?.members.map((member) => member.proxy_id) ?? [],
      expected: group?.revision ?? null,
    });
    setSearch("");
    setMessage("");
    setReview(false);
    setReadback(false);
    setOpen(true);
  }
  async function save(members: boolean) {
    if (!editor || lock.current || review || readback || createUnknown) return;
    const original = editor,
      current = sequence.current;
    if (!original.name.trim() || [...original.name.trim()].length > 80) {
      setMessage("分组名称应为 1–80 个字符");
      return;
    }
    if (original.ids.length > 500 || !original.ids.every(validID)) {
      setMessage("成员编号无效或超过 500 个");
      return;
    }
    lock.current = true;
    setSaving(true);
    setMessage("");
    try {
      if (!original.group) {
        const created = await adapter.createGroup({ name: original.name.trim(), enabled: original.enabled });
        if (!validID(created.id) || !validRevision(created.revision)) throw new Error("创建响应缺少有效编号或版本");
        if (!mounted.current || current !== sequence.current) return;
        setEditor({ ...original, group: created, expected: created.revision });
        setGroups((values) => [...values.filter((group) => group.id !== created.id), created]);
        setMessage("分组已创建。请在下方选择已有代理，再明确保存成员；尚未授予任何主体使用权限。");
      } else {
        const id = original.group.id,
          revision = original.expected;
        if (!validRevision(revision)) throw new Error("分组版本无效，请回读核对");
        const result = await submission.current.submit(
          async () => {
            if (members) await adapter.members(id, revision, original.ids);
            else
              await adapter.updateGroup(id, {
                expected_revision: revision,
                name: original.name.trim(),
                enabled: original.enabled,
              });
          },
          () => adapter.group(id),
          (value) =>
            value.id === id &&
            value.revision > revision &&
            (members
              ? JSON.stringify(value.members.map((member) => member.proxy_id)) === JSON.stringify(original.ids)
              : value.name === original.name.trim() && value.enabled === original.enabled),
        );
        if (!mounted.current || current !== sequence.current || !result) return;
        setGroups((values) => values.map((group) => (group.id === result.id ? result : group)));
        setEditor({ ...original, group: result, expected: result.revision });
        setMessage(
          submission.current.confirmed
            ? members
              ? "成员已保存并回读确认。仅保存代理编号，不复制 URL 或凭据。"
              : "名称与启用状态已保存并回读确认。"
            : "已回读当前分组。草稿保留，请核对后采用当前版本。",
        );
      }
    } catch (failure) {
      if (!mounted.current || current !== sequence.current) return;
      const status = (failure as { status?: number })?.status;
      if (!original.group && (!status || status >= 500 || status === 202)) setCreateUnknown(true);
      setMessage(
        `${status === 409 ? "分组冲突或成员正在使用，草稿已保留" : "保存结果需核对"}：${proxyErrorText(failure)}。不会自动重发。`,
      );
    } finally {
      lock.current = false;
      if (mounted.current && current === sequence.current) {
        setSaving(false);
        sync();
      }
    }
  }
  async function reconcile() {
    if (lock.current || !editor) return;
    lock.current = true;
    setSaving(true);
    const current = sequence.current;
    try {
      if (!editor.group) {
        const values = await adapter.groups();
        if (mounted.current && current === sequence.current) {
          setGroups(values);
          setMessage(
            "已刷新列表。创建响应未知，不能按名称推断成功。请核对列表，选择已建档分组继续编辑；不会再次创建。",
          );
        }
      } else {
        const id = editor.group.id;
        const result = await submission.current.reconcile(() => adapter.group(id));
        if (result && mounted.current && current === sequence.current) {
          setGroups((values) => values.map((group) => (group.id === result.id ? result : group)));
          setEditor({ ...editor, group: result });
          setMessage(`已核对当前版本 ${result.revision}，草稿与打开时版本仍保留。请明确采用当前版本后保存。`);
        }
      }
    } catch (failure) {
      if (mounted.current && current === sequence.current) {
        submission.current.needsReadback = true;
        setMessage(`核对失败：${proxyErrorText(failure)}。仍不重发写入。`);
      }
    } finally {
      lock.current = false;
      if (mounted.current) {
        setSaving(false);
        sync();
      }
    }
  }
  async function remove() {
    if (!deleting || lock.current) return;
    lock.current = true;
    setSaving(true);
    try {
      if (!deleteUnknown) await adapter.deleteGroup(deleting.id, deleting.revision);
      const values = await adapter.groups();
      if (!mounted.current) return;
      setGroups(values);
      if (values.some((group) => group.id === deleting.id)) {
        setDeleteUnknown(true);
        setMessage("分组仍存在，已保留当前记录。请关闭后核对版本；不会重复删除。");
      } else {
        setDeleting(null);
        setDeleteUnknown(false);
        setMessage("已核对分组删除结果。");
      }
    } catch (failure) {
      if (mounted.current) {
        setDeleteUnknown(true);
        setMessage(`删除未完成确认：${proxyErrorText(failure)}。仅允许刷新核对；引用中的组不能删除。`);
      }
    } finally {
      lock.current = false;
      if (mounted.current) setSaving(false);
    }
  }
  const visible = proxies.filter((proxy) =>
    `${proxy.id} ${proxy.name} ${proxy.kind}`.toLowerCase().includes(search.toLowerCase()),
  );
  return (
    <>
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle>代理分组</CardTitle>
              <CardDescription>维护名称、启用状态与成员。此处管理目录；任务和对话须分别明确授权使用。</CardDescription>
            </div>
            <div className="flex gap-2">
              <Button variant="outline" disabled={loading || saving} onClick={() => void refresh()}>
                刷新分组
              </Button>
              <Button disabled={loading || saving} onClick={() => begin(null)}>
                {createUnknown ? "核对未确认的创建" : "新建分组"}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {loading && <p role="status">正在加载分组…</p>}
          {listError && (
            <p role="alert" className="text-destructive">
              {listError}
            </p>
          )}
          {!loading && !listError && groups.length === 0 && (
            <p className="text-muted-foreground text-sm">暂无分组。已有代理可同时加入多个组。</p>
          )}
          {groups.map((group) => (
            <div key={group.id} className="flex flex-wrap items-center justify-between gap-3 rounded-lg border p-3">
              <div className="space-y-1">
                <p className="flex items-center gap-2 font-medium">
                  #{group.id} {group.name}
                  <Badge variant={group.enabled ? "secondary" : "outline"}>{group.enabled ? "已启用" : "已停用"}</Badge>
                </p>
                <p className="text-muted-foreground text-xs">
                  版本 {group.revision} · {group.members.length} 个成员 ·{" "}
                  {group.members.map((member) => `#${member.proxy_id}`).join("、") || "尚无成员"}
                </p>
              </div>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={saving}
                  onClick={() => {
                    begin(group);
                  }}
                >
                  管理分组
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={saving}
                  onClick={() => {
                    setDeleting(group);
                    setDeleteUnknown(false);
                    setMessage("");
                  }}
                >
                  删除
                </Button>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
      <Dialog
        open={open}
        onOpenChange={(value) => {
          if (!lock.current) setOpen(value);
        }}
      >
        <DialogContent className="sm:max-w-2xl" showCloseButton={!saving}>
          <DialogHeader>
            <DialogTitle>{editor?.group ? `管理分组 #${editor.group.id}` : "新建代理分组"}</DialogTitle>
            <DialogDescription>
              成员有序保存为编号，可属于多个组；保存分组不会自动授权 Agent 或主体使用。
            </DialogDescription>
          </DialogHeader>
          {editor && (
            <>
              <fieldset disabled={saving || readback || review || createUnknown} className="space-y-3">
                <Label className="grid gap-2">
                  分组名称
                  <Input
                    maxLength={80}
                    value={editor.name}
                    onChange={(event) => setEditor({ ...editor, name: event.target.value })}
                  />
                </Label>
                <div className="flex items-center gap-3">
                  <Switch
                    id="proxy-group-enabled"
                    checked={editor.enabled}
                    onCheckedChange={(enabled) => setEditor({ ...editor, enabled })}
                  />
                  <Label htmlFor="proxy-group-enabled">启用分组</Label>
                </div>
                <Button disabled={saving || readback || review || createUnknown} onClick={() => void save(false)}>
                  {editor.group ? "保存名称与启用状态" : "创建分组"}
                </Button>
              </fieldset>
              {editor.group && (
                <fieldset disabled={saving || readback || review} className="space-y-3 rounded-lg border p-3">
                  <p className="font-medium">选择已有代理 · 已选 {editor.ids.length} / 500</p>
                  <Input
                    aria-label="搜索现有代理"
                    placeholder="搜索编号、名称或类型"
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                  />
                  <div className="max-h-64 space-y-2 overflow-y-auto">
                    {visible.map((proxy) => (
                      <Label key={proxy.id} className="flex items-start gap-2 rounded-md border p-2 font-normal">
                        <Checkbox
                          checked={editor.ids.includes(String(proxy.id))}
                          disabled={proxy.kind === "resin" && !editor.ids.includes(String(proxy.id))}
                          onCheckedChange={(checked) =>
                            setEditor({
                              ...editor,
                              ids:
                                checked === true
                                  ? [...editor.ids, String(proxy.id)]
                                  : editor.ids.filter((id) => id !== String(proxy.id)),
                            })
                          }
                        />
                        <span>
                          #{proxy.id} {proxy.name} · {proxy.kind.toUpperCase()}
                          {proxyUnavailable(proxy, "group") && (
                            <span className="text-muted-foreground block text-xs">
                              {proxyUnavailable(proxy, "group")}
                            </span>
                          )}
                        </span>
                      </Label>
                    ))}
                  </div>
                  <p className="text-muted-foreground text-xs">
                    目录可维护离线成员；执行时只选合格成员。Resin
                    组租约暂不支持，不能作为切换候选。移除在用或预留成员会被服务端拒绝。
                  </p>
                  <Button variant="outline" disabled={saving || readback || review} onClick={() => void save(true)}>
                    保存成员编号
                  </Button>
                </fieldset>
              )}
            </>
          )}
          {message && (
            <p role="status" className="text-sm">
              {message}
            </p>
          )}
          {review && !readback && editor?.group && (
            <Button
              variant="outline"
              disabled={saving}
              onClick={() => {
                if (editor.group && submission.current.confirmNewIntent()) {
                  setEditor({ ...editor, expected: editor.group.revision });
                  sync();
                  setMessage("已采用当前分组版本，草稿保留。请再次明确保存。");
                }
              }}
            >
              核对完成，保留草稿并采用当前版本
            </Button>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={saving} onClick={() => setOpen(false)}>
              关闭
            </Button>
            <Button variant="outline" disabled={saving} onClick={() => void reconcile()}>
              仅刷新核对
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog
        open={deleting !== null}
        onOpenChange={(value) => {
          if (!value && !lock.current) setDeleting(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>删除分组「{deleting?.name}」？</DialogTitle>
            <DialogDescription>
              不会删除代理档案。仍被任务或对话授权引用的组无法删除。使用打开时版本 {deleting?.revision} 核对。
            </DialogDescription>
          </DialogHeader>
          {message && <p role="status">{message}</p>}
          <DialogFooter>
            <Button variant="outline" disabled={saving} onClick={() => setDeleting(null)}>
              取消
            </Button>
            <Button variant={deleteUnknown ? "outline" : "destructive"} disabled={saving} onClick={() => void remove()}>
              {deleteUnknown ? "仅刷新核对" : "确认删除分组"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
