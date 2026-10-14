package agent

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)

func TestMainAgentFollowupAdmissionFailureRetainsIdentity(t *testing.T) {
	d := testDB(t)
	defer d.Close()
	exp, err := d.CreateExploration("追加准入失败", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, exp)
	ts := NewToolSet(d.Exploration(exp), "human")
	ts.resumeTaskChecked = func() error { return errors.New("合成调度故障") }
	notifications := 0
	ts.SetNotify(func() { notifications++ })
	ts.SetNotifyHint(func([]string) { notifications++ })
	ts.SetNotifyGoal(func([]string) { notifications++ })
	for _, name := range []string{"add_intent", "set_goals", "add_hint"} {
		var result string
		for _, tool := range ts.MainAgentTools() {
			if tool.Name() == name {
				input := json.RawMessage(`{"summary":"追加核验","text":"新增验收目标"}`)
				res, err := tool.Call(t.Context(), input, nil)
				if err != nil {
					t.Fatal(err)
				}
				result = res.Flatten()
			}
		}
		if !strings.Contains(result, "ids=[") || !strings.Contains(result, "尚未开始执行") || !strings.Contains(result, "不要重复登记") {
			t.Fatalf("%s 假报成功或丢失身份: %s", name, result)
		}
	}
	intents, _ := ts.ts.ListByKind(db.KindIntent, 100)
	goals, _ := ts.ts.ListByKind(db.KindGoal, 100)
	if len(intents) != 1 || len(goals) != 3 {
		t.Fatalf("已登记工作丢失: intents=%d goals=%d", len(intents), len(goals))
	}
	if notifications != 0 {
		t.Fatal("准入失败仍通知 Planner")
	}
}

func TestMainAgentFollowupBatchAdmissionBeforeNotify(t *testing.T) {
	d := testDB(t)
	defer d.Close()
	exp, err := d.CreateExploration("批量追加", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, exp)
	for _, name := range []string{"add_hint", "set_goals"} {
		ts := NewToolSet(d.Exploration(exp), "human")
		admitted, notified := false, 0
		ts.SetResumeTaskChecked(func() error { admitted = true; return nil })
		notify := func(texts []string) {
			if !admitted || len(texts) != 1 {
				t.Fatalf("通知先于准入或包含失败项: %v %v", admitted, texts)
			}
			notified++
		}
		ts.SetNotifyGoal(notify)
		ts.SetNotifyHint(notify)
		input := `{"goals":[{"text":""},{"text":"追加目标"}]}`
		tool := ts.SetGoalsTool()
		if name == "add_hint" {
			input = `{"hints":[{"text":""},{"text":"追加方向"}]}`
			tool = ts.AddHintTool()
		}
		res, err := tool.Call(t.Context(), json.RawMessage(input), nil)
		var out struct {
			IDs    []int64           `json:"ids"`
			Errors map[string]string `json:"errors"`
		}
		if err != nil || res.IsError || json.Unmarshal([]byte(res.Flatten()), &out) != nil || len(out.IDs) != 2 || out.IDs[0] != 0 || out.IDs[1] <= 0 || out.Errors["0"] == "" || notified != 1 {
			t.Fatalf("%s 批量身份或通知错误: %s %v", name, res.Flatten(), err)
		}
	}
}
