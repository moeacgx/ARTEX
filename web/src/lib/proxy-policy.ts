import type { ProxyRecord } from "./types";
import { warpEffectiveIP } from "./warp-health";

export type Subject = { kind: "task" | "conversation"; id: string };
export type ProxyMode = "inherit" | "direct" | "fixed" | "group";
export type SwitchState = "pending" | "draining" | "applying" | "succeeded" | "failed" | "recovery_required";
export type ProxySwitch = {
  id: string;
  subject_kind: Subject["kind"];
  subject_id: string;
  base_revision: number;
  authorization_revision: number;
  state: SwitchState;
  from_proxy_id: string | null;
  to_proxy_id: string | null;
  reason: string;
  error_code: string;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  result_revision: number | null;
};
export type ProxyGroup = {
  id: string;
  name: string;
  enabled: boolean;
  revision: number;
  members: { proxy_id: string; position: number; proxy: ProxyRecord }[];
};
export type ProxyPolicy = {
  subject_kind: Subject["kind"];
  subject_id: string;
  proxy_mode: ProxyMode;
  proxy_id: string | null;
  proxy_revision: number;
  allowed_group_ids: string[];
  agent_failover_enabled: boolean;
  failover_cooldown_seconds: number;
  failover_max_switches: number;
  authorization_revision: number;
  proxy: ProxyRecord | null;
  exit_verified_at: string | null;
  latest_switch: ProxySwitch | null;
  cooldown_until: string | null;
  remaining_switches: number;
  recovery_required: boolean;
};
export type PolicyDraft = {
  mode: ProxyMode;
  proxyID: string;
  groupIDs: string[];
  agentEnabled: boolean;
  cooldown: string;
  budget: string;
};
export type PolicyInput = {
  expected_revision: number;
  proxy_mode: ProxyMode;
  proxy_id: string | null;
  allowed_group_ids: string[];
  agent_failover_enabled: boolean;
  failover_cooldown_seconds: number;
  failover_max_switches: number;
};
export type FailoverInput = {
  expected_revision: number;
  reason: string;
  evidence: {
    kind: "connection_failure" | "tunnel_failure" | "process_failure" | "health_confirmation";
    reference: string;
  };
  target_proxy_id?: string;
};
export type PolicyRecoveryAction = "retry" | "replace_policy" | "confirm_released" | "confirm_released_replace_policy";
export type PolicyRecoveryInput =
  | { expected_revision: number; action: "retry" | "confirm_released" }
  | (PolicyInput & { action: "replace_policy" | "confirm_released_replace_policy" });

export const SWITCH_LABEL: Record<SwitchState, string> = {
  pending: "处理中",
  draining: "等待操作排空",
  applying: "执行切换中",
  succeeded: "切换已完成",
  failed: "切换失败",
  recovery_required: "待人工恢复",
};
export const validRevision = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
export const validID = (value: unknown): value is string => typeof value === "string" && /^[1-9]\d*$/.test(value);
export const switchPending = (value: ProxySwitch | null) =>
  !!value && ["pending", "draining", "applying"].includes(value.state);
export const policyRecoveryRequired = (policy: ProxyPolicy | null) =>
  !!policy && (policy.recovery_required || policy.latest_switch?.state === "recovery_required");
export const recoveryReplacesPolicy = (action: PolicyRecoveryAction) =>
  action === "replace_policy" || action === "confirm_released_replace_policy";
export const recoveryConfirmsLease = (action: PolicyRecoveryAction) =>
  action === "confirm_released" || action === "confirm_released_replace_policy";

export function draftFromPolicy(policy: ProxyPolicy): PolicyDraft {
  return {
    mode: policy.proxy_mode,
    proxyID: policy.proxy_id ?? "",
    groupIDs: [...policy.allowed_group_ids],
    agentEnabled: policy.agent_failover_enabled,
    cooldown: String(policy.failover_cooldown_seconds),
    budget: String(policy.failover_max_switches),
  };
}

export function proxyUnavailable(proxy: ProxyRecord, scope: "fixed-task" | "fixed-conversation" | "group"): string {
  if (proxy.kind === "resin" && scope !== "fixed-task")
    return scope === "group"
      ? "Resin 暂不支持分组租约（group_resin_lease_scope_unsupported）"
      : "Resin 暂不支持对话租约（conversation_resin_lease_scope_unsupported）";
  if (!proxy.enabled) return "代理已停用";
  if (proxy.status !== "healthy") return "尚未通过连接检测";
  if (proxy.kind === "warp" && !warpEffectiveIP(proxy)) return "WARP 未在线或出口未验证";
  return "";
}

export function groupUnavailable(group: ProxyGroup): string {
  if (!group.enabled) return "分组已停用";
  if (!group.members.some((member) => !proxyUnavailable(member.proxy, "group")))
    return "组内没有已验证且受支持的成员；不会回落直连";
  return "";
}

export function allowedCandidates(policy: ProxyPolicy | null, groups: ProxyGroup[]) {
  if (!policy || policy.proxy_mode !== "group") return [];
  const candidates = new Map<string, ProxyGroup["members"][number]>();
  for (const group of groups) {
    if (!group.enabled || !policy.allowed_group_ids.includes(group.id)) continue;
    for (const member of group.members) {
      if (!proxyUnavailable(member.proxy, "group")) candidates.set(member.proxy_id, member);
    }
  }
  return [...candidates.values()];
}

export function policyInput(draft: PolicyDraft, revision: number): PolicyInput {
  if (!validRevision(revision)) throw new Error("策略版本无效，请重新核对");
  if (!["inherit", "direct", "fixed", "group"].includes(draft.mode)) throw new Error("请选择连接方式");
  if (draft.mode === "fixed" && !validID(draft.proxyID)) throw new Error("请选择有效代理编号");
  if (
    draft.groupIDs.length > 100 ||
    new Set(draft.groupIDs).size !== draft.groupIDs.length ||
    !draft.groupIDs.every(validID)
  )
    throw new Error("授权组编号无效或重复");
  if (draft.mode === "group" && !draft.groupIDs.length) throw new Error("分组模式至少需要授权一个分组");
  const cooldown = Number(draft.cooldown),
    budget = Number(draft.budget);
  if (!/^\d+$/.test(draft.cooldown) || !Number.isSafeInteger(cooldown) || cooldown > 86400)
    throw new Error("冷却时间应为 0–86400 秒");
  if (!/^\d+$/.test(draft.budget) || !Number.isSafeInteger(budget) || budget > 1000)
    throw new Error("切换预算应为 0–1000 次");
  return {
    expected_revision: revision,
    proxy_mode: draft.mode,
    proxy_id: draft.mode === "fixed" ? draft.proxyID : null,
    allowed_group_ids: [...draft.groupIDs],
    agent_failover_enabled: draft.agentEnabled,
    failover_cooldown_seconds: cooldown,
    failover_max_switches: budget,
  };
}

export function policyDraftError(
  draft: PolicyDraft,
  revision: number,
  subject: Subject,
  groups: ProxyGroup[],
  proxies: ProxyRecord[],
): string {
  try {
    policyInput(draft, revision);
  } catch (error) {
    return (error as Error).message;
  }
  if (draft.mode === "fixed") {
    const proxy = proxies.find((item) => String(item.id) === draft.proxyID);
    return proxy
      ? proxyUnavailable(proxy, subject.kind === "task" ? "fixed-task" : "fixed-conversation")
      : "指定代理已缺失";
  }
  if (draft.mode === "group") {
    const selected = groups.filter((group) => draft.groupIDs.includes(group.id));
    if (selected.length !== draft.groupIDs.length) return "授权组记录缺失，请刷新核对或撤销该授权";
    if (!selected.some((group) => !groupUnavailable(group))) return "获准组没有合格成员，不会回落直连";
  }
  return "";
}

// 恢复原目标不采样新策略。租约确认只选择正式 action，不能作为进程退出证明发送给服务端。
export function policyRecoveryInput(
  action: PolicyRecoveryAction,
  revision: number,
  draft: PolicyDraft,
  leaseChecked = false,
): PolicyRecoveryInput {
  if (!validRevision(revision)) throw new Error("恢复版本无效，请重新核对");
  if (!["retry", "replace_policy", "confirm_released", "confirm_released_replace_policy"].includes(action))
    throw new Error("请选择正式恢复操作");
  if (recoveryConfirmsLease(action) && !leaseChecked) throw new Error("请先人工核对远端原租约已释放");
  if (action === "replace_policy" || action === "confirm_released_replace_policy")
    return { ...policyInput(draft, revision), action };
  return { expected_revision: revision, action };
}

export function policyRecoveryMatches(policy: ProxyPolicy, input: PolicyRecoveryInput): boolean {
  if (policyRecoveryRequired(policy) || switchPending(policy.latest_switch)) return false;
  if (input.action === "replace_policy" || input.action === "confirm_released_replace_policy")
    return policyMatches(policy, input);
  return policy.proxy_revision >= input.expected_revision;
}

// 只展示当前主体的有效检测结果；历史档案或正在切换时的旧 IP 不属于有效出口。
export function effectiveExit(policy: ProxyPolicy | null, uncertain = false): string | null {
  if (
    !policy ||
    uncertain ||
    policyRecoveryRequired(policy) ||
    !policy.exit_verified_at ||
    !Number.isFinite(Date.parse(policy.exit_verified_at))
  )
    return null;
  if (policy.latest_switch && policy.latest_switch.state !== "succeeded") return null;
  const proxy = policy.proxy;
  if (!proxy || proxyUnavailable(proxy, policy.subject_kind === "task" ? "fixed-task" : "fixed-conversation"))
    return null;
  return proxy.kind === "warp" ? warpEffectiveIP(proxy) : proxy.egress_ip?.trim() || null;
}

export function cooldownRemaining(until: string | null, now: number): number {
  if (!until || !Number.isFinite(Date.parse(until))) return 0;
  return Math.max(0, Math.ceil((Date.parse(until) - now) / 1000));
}

export function policyMatches(policy: ProxyPolicy, input: PolicyInput): boolean {
  return (
    policy.proxy_revision > input.expected_revision &&
    policy.proxy_mode === input.proxy_mode &&
    (input.proxy_mode !== "fixed" || String(policy.proxy_id) === input.proxy_id) &&
    policy.agent_failover_enabled === input.agent_failover_enabled &&
    policy.failover_cooldown_seconds === input.failover_cooldown_seconds &&
    policy.failover_max_switches === input.failover_max_switches &&
    JSON.stringify(policy.allowed_group_ids) === JSON.stringify(input.allowed_group_ids)
  );
}

export type WriteReceipt = { pending?: ProxySwitch } | void;

// 每个编辑会话持有独立同步锁。冲突或未知结果只允许 GET，必须人工核对后才能提交新意图。
export class RevisionSubmission<T> {
  busy = false;
  needsReadback = false;
  reviewRequired = false;
  confirmed = false;
  pending: ProxySwitch | null = null;
  private acknowledged = false;

  confirmNewIntent() {
    if (this.busy || this.needsReadback || this.pending) return false;
    this.reviewRequired = false;
    this.confirmed = false;
    return true;
  }

  async submit(
    write: () => Promise<WriteReceipt>,
    read: () => Promise<T>,
    matches: (value: T) => boolean,
  ): Promise<T | null> {
    if (this.busy) return null;
    if (this.reviewRequired && !this.needsReadback) throw new Error("请先核对当前状态并明确确认新的保存意图");
    this.busy = true;
    try {
      if (!this.needsReadback) {
        this.confirmed = false;
        this.acknowledged = false;
        try {
          const receipt = await write();
          this.pending = receipt?.pending ?? null;
          this.acknowledged = !this.pending;
          this.needsReadback = true;
        } catch (error) {
          const status = (error as { status?: number })?.status;
          if (!status || status >= 500 || status === 409 || status === 202) {
            this.needsReadback = true;
            this.reviewRequired = true;
          }
          throw error;
        }
      }
      const value = await read();
      this.needsReadback = false;
      this.confirmed = this.acknowledged && matches(value);
      this.reviewRequired = !this.confirmed;
      return value;
    } finally {
      this.busy = false;
    }
  }

  async reconcile(read: () => Promise<T>): Promise<T | null> {
    if (this.busy) return null;
    this.busy = true;
    try {
      const value = await read();
      this.needsReadback = false;
      this.reviewRequired = true;
      return value;
    } finally {
      this.busy = false;
    }
  }
}
