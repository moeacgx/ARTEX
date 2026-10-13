package db

import (
	"sync"
	"testing"
)

func TestHumanIntentGoalAtomicAndConcurrent(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	exp, err := d.CreateExploration("人工追加", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, exp)
	s := d.Exploration(exp)
	if _, err = s.AddIntent(map[string]any{"summary": "失败项"}, 1, []int64{-1}, "human"); err == nil {
		t.Fatal("无效资产引用未拒绝")
	}
	goals, _ := s.ListByKind(KindGoal, 100)
	if len(goals) != 0 {
		t.Fatal("失败意图留下孤立目标")
	}
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.AddIntent(map[string]any{"summary": "追加执行"}, 1, nil, "human")
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	intents, _ := s.ListByKind(KindIntent, 100)
	goals, _ = s.ListByKind(KindGoal, 100)
	if len(intents) != 2 || len(goals) != 2 {
		t.Fatalf("并发登记不完整: intents=%d goals=%d", len(intents), len(goals))
	}
	var edges int
	if err = d.QueryRow(`SELECT count(*) FROM exploration_edges WHERE exploration_id=$1 AND rel='spawns'`, exp).Scan(&edges); err != nil || edges != 2 {
		t.Fatalf("追加目标未连到意图: %d %v", edges, err)
	}
	if _, err = s.AddIntent(map[string]any{"summary": "Planner 常规规划"}, 1, nil, "planner"); err != nil {
		t.Fatal(err)
	}
	goals, _ = s.ListByKind(KindGoal, 100)
	if len(goals) != 2 {
		t.Fatal("Planner 产出意图不应不断扩张验收范围")
	}
}
