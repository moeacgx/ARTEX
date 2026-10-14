package db

import (
	"encoding/json"
	"testing"
)

func TestFollowupHintAtomicGoalAndOrdinaryHint(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	exp, err := d.CreateExploration("追加提示原子登记", "旧成果")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, exp)
	s := d.Exploration(exp)
	if _, err := s.AddNode(KindHint, map[string]any{"text": "普通信息"}, 0, "active", "human", nil); err != nil {
		t.Fatal(err)
	}
	if goals, err := s.ListByKind(KindGoal, 100); err != nil || len(goals) != 0 {
		t.Fatalf("普通提示产生执行目标: %v %v", goals, err)
	}
	id, err := s.AddFollowupHint(map[string]any{"text": "追加核验"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	goals, err := s.ListByKind(KindGoal, 100)
	if err != nil || len(goals) != 1 || goals[0].State != "open" {
		t.Fatalf("追加目标未登记: %v %v", goals, err)
	}
	var payload struct {
		HintID int64  `json:"hint_id"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal(goals[0].Payload, &payload); err != nil || payload.HintID != id || payload.Text != "完成追加方向：追加核验" {
		t.Fatalf("追加身份不一致: %+v %v", payload, err)
	}
	// 锚点外键失败必须回滚提示和验收目标，不能留下半份追加工作。
	if _, err := s.AddFollowupHint(map[string]any{"text": "应回滚"}, []int64{-1}); err == nil {
		t.Fatal("无效锚点未拒绝")
	}
	hints, err := s.ListByKind(KindHint, 100)
	if err != nil || len(hints) != 2 {
		t.Fatalf("失败遗留提示: %v %v", hints, err)
	}
	goals, err = s.ListByKind(KindGoal, 100)
	if err != nil || len(goals) != 1 {
		t.Fatalf("失败遗留目标: %v %v", goals, err)
	}
}
