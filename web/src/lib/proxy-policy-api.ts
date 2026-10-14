import { APIError, http } from "./api";
import {
  type FailoverInput,
  type PolicyInput,
  type PolicyRecoveryInput,
  type ProxyGroup,
  type ProxyPolicy,
  type ProxySwitch,
  type Subject,
  validID,
  validRevision,
} from "./proxy-policy";

const root = (subject: Subject) =>
  `/${subject.kind === "task" ? "tasks" : "conversations"}/${encodeURIComponent(subject.id)}/proxy`;
const json = (method: string, body: unknown): RequestInit => ({ method, body: JSON.stringify(body) });
function revision(value: number) {
  if (!validRevision(value)) throw new Error("缺少有效版本，请重新读取");
}
function groupDTO(value: ProxyGroup): ProxyGroup {
  if (
    !value ||
    !validID(value.id) ||
    !validRevision(value.revision) ||
    typeof value.name !== "string" ||
    typeof value.enabled !== "boolean" ||
    !Array.isArray(value.members) ||
    !value.members.every(
      (member) => validID(member.proxy_id) && member.proxy && String(member.proxy.id) === member.proxy_id,
    )
  )
    throw new Error("分组回读编号、版本或成员无效");
  return value;
}

export const proxyPolicyAPI = {
  groups: () =>
    http<{ groups: ProxyGroup[] }>("/proxy-groups", { cache: "no-store" }).then((value) => {
      if (!Array.isArray(value.groups)) throw new Error("分组目录响应无效");
      return value.groups.map(groupDTO);
    }),
  group: async (id: string) => {
    const value = groupDTO(await http<ProxyGroup>(`/proxy-groups/${encodeURIComponent(id)}`, { cache: "no-store" }));
    if (value.id !== id) throw new Error("分组回读编号不一致");
    return value;
  },
  createGroup: (input: { name: string; enabled: boolean }) => http<ProxyGroup>("/proxy-groups", json("POST", input)),
  updateGroup: (id: string, input: { expected_revision: number; name: string; enabled: boolean }) => {
    revision(input.expected_revision);
    return http<ProxyGroup>(`/proxy-groups/${encodeURIComponent(id)}`, json("PUT", input));
  },
  members: (id: string, expectedRevision: number, proxyIDs: string[]) => {
    revision(expectedRevision);
    return http<ProxyGroup>(
      `/proxy-groups/${encodeURIComponent(id)}/members`,
      json("PUT", { expected_revision: expectedRevision, proxy_ids: proxyIDs }),
    );
  },
  deleteGroup: (id: string, expectedRevision: number) => {
    revision(expectedRevision);
    return http<void>(`/proxy-groups/${encodeURIComponent(id)}?expected_revision=${expectedRevision}`, {
      method: "DELETE",
    });
  },
  policy: async (subject: Subject) => {
    const value = await http<ProxyPolicy>(root(subject), { cache: "no-store" });
    if (
      value.subject_kind !== subject.kind ||
      String(value.subject_id) !== subject.id ||
      !validRevision(value.proxy_revision) ||
      !["inherit", "direct", "fixed", "group"].includes(value.proxy_mode) ||
      !Array.isArray(value.allowed_group_ids) ||
      !value.allowed_group_ids.every(validID) ||
      typeof value.agent_failover_enabled !== "boolean" ||
      !validRevision(value.failover_cooldown_seconds) ||
      !validRevision(value.failover_max_switches) ||
      !validRevision(value.remaining_switches) ||
      typeof value.recovery_required !== "boolean"
    )
      throw new Error("策略回读主体或版本无效");
    return value;
  },
  save: async (subject: Subject, input: PolicyInput) => {
    revision(input.expected_revision);
    // 202 被保留为处理中，不能作为策略保存成功。
    await http<unknown>(root(subject), json("PUT", input), true);
  },
  recover: async (subject: Subject, input: PolicyRecoveryInput) => {
    revision(input.expected_revision);
    await http<unknown>(`${root(subject)}/policy-recovery`, json("POST", input), true);
  },
  request: async (subject: Subject, input: FailoverInput): Promise<ProxySwitch> => {
    revision(input.expected_revision);
    try {
      await http<unknown>(`${root(subject)}/switch-requests`, json("POST", input), true);
    } catch (error) {
      if (error instanceof APIError && error.status === 202) {
        const value = (error.payload as { request?: ProxySwitch })?.request;
        if (
          value &&
          value.subject_kind === subject.kind &&
          String(value.subject_id) === subject.id &&
          ["pending", "draining", "applying"].includes(value.state)
        )
          return value;
      }
      throw error;
    }
    throw new Error("切换请求未返回约定的 202 处理中状态，请仅刷新核对");
  },
  switchRequest: async (subject: Subject, id: string) => {
    const value = await http<ProxySwitch>(`${root(subject)}/switch-requests/${encodeURIComponent(id)}`, {
      cache: "no-store",
    });
    if (value.subject_kind !== subject.kind || String(value.subject_id) !== subject.id || String(value.id) !== id)
      throw new Error("切换回读主体不一致");
    return value;
  },
};

export type ProxyPolicyAdapter = typeof proxyPolicyAPI;
