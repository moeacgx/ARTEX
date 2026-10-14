package server

import (
	"context"
	"encoding/json"
	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/permission"
	actool "github.com/Autumn-27/norma/tool"
)

func isProxyFailoverTool(name string) bool {
	return name == "list_allowed_proxies" || name == "request_proxy_failover"
}
func proxyFailoverSchema() map[string]any {
	return proxyObject(map[string]any{
		"expected_revision": proxyInteger("本主体当前代理版本", 0, proxySafeIntegerMax()), "reason": strParam("切换理由；文本不是授权或故障证明"),
		"evidence": proxyObject(map[string]any{"kind": map[string]any{"type": "string", "enum": []string{"connection_failure", "tunnel_failure", "process_failure", "health_confirmation"}}, "reference": strParam("正式运行器记录的故障证据 ID")}, "kind", "reference"), "target_proxy_id": strParam("可选获准候选代理字符串 ID"),
	}, "expected_revision", "reason", "evidence")
}
func (s *Server) proxyFailoverTools() []actool.CoreTool {
	build := func(name, desc string, schema map[string]any, readOnly bool, run func(context.Context, json.RawMessage) (actool.Result, error)) actool.CoreTool {
		return actool.Build(actool.Spec{Name: name, Description: desc, Schema: schema, RawInput: true, ReadOnly: func(json.RawMessage) bool { return readOnly }, Concurrent: func(json.RawMessage) bool { return readOnly }, Permissions: func(ctx context.Context, _ json.RawMessage, _ permission.Context) permission.Decision {
			subject, e := s.proxySubjectFromContext(ctx)
			if e != nil {
				return permission.Denied("缺少正式主体上下文")
			}
			p, e := s.m.pg.GetProxyPolicy(subject)
			if e != nil || p.State.Mode != db.TaskProxyModeGroup || (!readOnly && !p.AgentFailoverEnabled) {
				return permission.Denied("主体没有显式使用授权")
			}
			return permission.Allowed()
		}, Run: func(ctx context.Context, raw json.RawMessage, _ *actool.ToolContext) (actool.Result, error) {
			if decodeProxyToolInput(raw, schema, nil) != nil {
				return actool.Errorf("代理工具参数无效"), nil
			}
			return run(ctx, raw)
		}})
	}
	return []actool.CoreTool{
		build("list_allowed_proxies", "查询本次正式任务或对话已获准分组并集，不接受模型提供主体 ID，不授予使用权限。", proxyObject(map[string]any{}), true, func(ctx context.Context, _ json.RawMessage) (actool.Result, error) {
			subject, e := s.proxySubjectFromContext(ctx)
			if e != nil {
				return actool.Errorf("主体未授权"), nil
			}
			p, candidates, e := s.m.pg.AllowedProxyCandidates(subject)
			if e != nil || p.State.Mode != db.TaskProxyModeGroup {
				return actool.Errorf("主体没有分组使用授权"), nil
			}
			policy, e := s.proxyPolicyDTO(p)
			if e != nil {
				return actool.Errorf("代理策略读取失败"), nil
			}
			out := []map[string]any{}
			for _, c := range candidates {
				groups := []string{}
				for _, id := range c.GroupIDs {
					groups = append(groups, i64s(id))
				}
				v := map[string]any{"proxy_id": i64s(c.Proxy.ID), "proxy": s.runtimeProxyDTO(c.Proxy), "group_ids": groups, "supported": c.Supported}
				if !c.Supported {
					v["unsupported_reason"] = c.UnsupportedReason
				}
				out = append(out, v)
			}
			evidence, e := s.m.pg.ProxyFailureEvidence(subject)
			if e != nil {
				return actool.Errorf("故障证据读取失败"), nil
			}
			return jsonResult(map[string]any{"policy": policy, "candidates": out, "failure_evidence": evidence})
		}),
		build("request_proxy_failover", "经用户显式授权和服务端基础设施故障证据校验登记异步切换，立即结束本轮；调度器等待清理退出。禁止重放有副作用工具；403/验证码/429不能作为轮换依据。", proxyFailoverSchema(), false, func(ctx context.Context, raw json.RawMessage) (actool.Result, error) {
			subject, e := s.proxySubjectFromContext(ctx)
			if e != nil {
				return actool.Errorf("主体未授权"), nil
			}
			var req proxyFailoverHTTPInput
			if json.Unmarshal(raw, &req) != nil {
				return actool.Errorf("切换参数无效"), nil
			}
			input, e := req.input()
			if e != nil {
				return actool.Errorf("切换参数无效"), nil
			}
			request, e := s.m.pg.RequestProxyFailover(subject, input)
			if e != nil {
				return actool.Errorf(e.Error()), nil
			}
			agent.RequestProxyRunStop(ctx)
			s.m.wakeProxyFailover()
			return jsonResult(map[string]any{"accepted": true, "request": proxySwitchDTO(request), "end_turn": true})
		}),
	}
}
