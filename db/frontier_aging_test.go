package db

import (
	"testing"
	"time"
)

func TestFrontierAgingPreventsContinuousPriorityStarvation(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	expID, err := d.CreateExploration("队列防饥饿回归", "旧意图必须获得执行机会")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, expID)
	store := d.Exploration(expID)
	old, err := store.AddIntent(map[string]any{"summary": "已经等待很久的方向"}, 4, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Exec(`UPDATE exploration_nodes SET created_at=now()-interval '12 hours' WHERE id=$1`, old); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		if _, err = store.AddIntent(map[string]any{"summary": "新来的高优先级方向"}, 10, nil, "planner"); err != nil {
			t.Fatal(err)
		}
	}
	frontier, err := store.Frontier(20)
	if err != nil || len(frontier) != 20 || frontier[0].ID != old {
		t.Fatalf("旧意图仍被新高优先级挤出领取窗口: first=%+v old=%d err=%v", frontier, old, err)
	}
	node, err := store.GetNode(old)
	if err != nil || node.State != "open" || node.Priority != 4 || node.Owner != "" {
		t.Fatalf("排队老化不应修改持久状态或优先级: node=%+v err=%v", node, err)
	}
}

func TestFrontierAgingPreservesFreshPriorityAndSaturatesToFIFO(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	expID, err := d.CreateExploration("队列老化边界回归", "新意图按优先级，等待后逐步提升")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, expID)
	store := d.Exploration(expID)
	add := func(priority int, age time.Duration) int64 {
		t.Helper()
		id, addErr := store.AddIntent(map[string]any{"summary": "排队方向"}, priority, nil, "planner")
		if addErr != nil {
			t.Fatal(addErr)
		}
		if _, addErr = d.Exec(`UPDATE exploration_nodes SET created_at=now()-($2 * interval '1 second') WHERE id=$1`, id, int64(age/time.Second)); addErr != nil {
			t.Fatal(addErr)
		}
		return id
	}
	low := add(5, 19*time.Minute)
	high := add(6, 0)
	frontier, err := store.Frontier(2)
	if err != nil || len(frontier) != 2 || frontier[0].ID != high {
		t.Fatalf("新排队意图应保留原优先级排序: nodes=%+v err=%v", frontier, err)
	}
	if _, err = d.Exec(`UPDATE exploration_nodes SET created_at=now()-interval '21 minutes' WHERE id=$1`, low); err != nil {
		t.Fatal(err)
	}
	frontier, err = store.Frontier(2)
	if err != nil || len(frontier) != 2 || frontier[0].ID != low {
		t.Fatalf("等待20分钟后应提升一级并同级FIFO: nodes=%+v err=%v", frontier, err)
	}
	if _, err = d.Exec(`UPDATE exploration_nodes SET created_at=now()-interval '12 hours' WHERE id IN ($1,$2)`, low, high); err != nil {
		t.Fatal(err)
	}
	frontier, err = store.Frontier(2)
	if err != nil || len(frontier) != 2 || frontier[0].ID != low {
		t.Fatalf("老化满级后必须按原先入队顺序: nodes=%+v err=%v", frontier, err)
	}
	if err = store.SetNodeState(low, "paused"); err != nil {
		t.Fatal(err)
	}
	frontier, err = store.Frontier(2)
	if err != nil || len(frontier) != 1 || frontier[0].ID != high {
		t.Fatalf("暂停意图不能被老化重新投递: nodes=%+v err=%v", frontier, err)
	}
}

func TestFrontierAgingHandlesFutureClockAndLegacyPriority(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	expID, err := d.CreateExploration("队列时钟及历史优先级回归", "异常数据不能破坏调度")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, expID)
	store := d.Exploration(expID)
	ids := make([]int64, 3)
	for i, priority := range []int{-2147483648, 10, 2147483647} {
		ids[i], err = store.AddIntent(map[string]any{"summary": "历史方向"}, priority, nil, "planner")
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = d.Exec(`UPDATE exploration_nodes SET created_at=now()+interval '1 day' WHERE id=$1`, ids[2]); err != nil {
		t.Fatal(err)
	}
	frontier, err := store.Frontier(3)
	if err != nil || len(frontier) != 3 || frontier[0].ID != ids[1] || frontier[1].ID != ids[2] {
		t.Fatalf("未来时钟或越界优先级改变正常排序: nodes=%+v err=%v", frontier, err)
	}
	if _, err = d.Exec(`UPDATE exploration_nodes SET created_at=now()-interval '12 hours' WHERE id=$1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	frontier, err = store.Frontier(3)
	if err != nil || len(frontier) != 3 || frontier[0].ID != ids[0] {
		t.Fatalf("历史负优先级不应永久饿死或溢出: nodes=%+v err=%v", frontier, err)
	}
	node, err := store.GetNode(ids[0])
	if err != nil || node.Priority != -2147483648 {
		t.Fatalf("调度归一不应改历史记录: node=%+v err=%v", node, err)
	}
}
