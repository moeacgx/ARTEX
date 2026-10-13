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
	for _, name := range []string{"add_intent", "set_goals"} {
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
	if len(intents) != 1 || len(goals) != 2 {
		t.Fatalf("已登记工作丢失: intents=%d goals=%d", len(intents), len(goals))
	}
}
