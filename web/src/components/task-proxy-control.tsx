"use client";

import * as React from "react";

import { ProxyPolicyControl } from "@/components/proxy-policy-control";
import { api } from "@/lib/api";
import type { Task } from "@/lib/types";

export function TaskProxyControl({
  task,
  onUpdated,
  onSaving,
}: {
  task: Task;
  onUpdated: (task: Task) => void;
  onSaving: () => void;
}) {
  const sequence = React.useRef(0);
  const mounted = React.useRef(true);
  React.useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      sequence.current++;
    };
  }, []);
  const saving = React.useCallback(() => {
    sequence.current++;
    onSaving();
  }, [onSaving]);
  const updated = React.useCallback(() => {
    const current = ++sequence.current;
    void api
      .task(String(task.id))
      .then((value) => {
        if (mounted.current && current === sequence.current && String(value.id) === String(task.id)) onUpdated(value);
      })
      .catch(() => undefined);
  }, [task.id, onUpdated]);
  return (
    <ProxyPolicyControl
      key={task.id}
      subject={{ kind: "task", id: String(task.id) }}
      onSaving={saving}
      onUpdated={updated}
    />
  );
}
