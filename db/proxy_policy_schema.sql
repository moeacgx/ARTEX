CREATE TABLE IF NOT EXISTS proxy_groups (
 id bigserial PRIMARY KEY, name text NOT NULL CHECK(char_length(name) BETWEEN 1 AND 80),
 enabled boolean NOT NULL DEFAULT true, revision bigint NOT NULL DEFAULT 0 CHECK(revision BETWEEN 0 AND 9007199254740991)
);
CREATE TABLE IF NOT EXISTS proxy_group_members (
 group_id bigint NOT NULL REFERENCES proxy_groups(id) ON DELETE CASCADE,
 proxy_id bigint NOT NULL REFERENCES outbound_proxies(id) ON DELETE RESTRICT,
 position integer NOT NULL CHECK(position>=0), PRIMARY KEY(group_id,proxy_id), UNIQUE(group_id,position)
);
CREATE INDEX IF NOT EXISTS proxy_group_members_proxy_idx ON proxy_group_members(proxy_id);
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS proxy_mode text NOT NULL DEFAULT 'inherit';
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE RESTRICT;
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS proxy_snapshot jsonb NOT NULL DEFAULT '{}';
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS proxy_revision bigint NOT NULL DEFAULT 0 CHECK(proxy_revision BETWEEN 0 AND 9007199254740991);
CREATE TABLE IF NOT EXISTS proxy_subject_policies (
 id bigserial PRIMARY KEY,
 task_id bigint UNIQUE REFERENCES tasks(id) ON DELETE CASCADE,
 conversation_id bigint UNIQUE REFERENCES conversations(id) ON DELETE CASCADE,
 agent_failover_enabled boolean NOT NULL DEFAULT false,
 failover_cooldown_seconds integer NOT NULL DEFAULT 60 CHECK(failover_cooldown_seconds BETWEEN 0 AND 86400),
 failover_max_switches integer NOT NULL DEFAULT 3 CHECK(failover_max_switches BETWEEN 0 AND 1000),
 authorization_revision bigint NOT NULL DEFAULT 0 CHECK(authorization_revision BETWEEN 0 AND 9007199254740991),
 switches_used integer NOT NULL DEFAULT 0 CHECK(switches_used>=0), cooldown_until timestamptz,
 exit_verified_at timestamptz, CHECK(num_nonnulls(task_id,conversation_id)=1)
);
ALTER TABLE proxy_subject_policies ADD COLUMN IF NOT EXISTS switch_token text NOT NULL DEFAULT '';
ALTER TABLE proxy_subject_policies ADD COLUMN IF NOT EXISTS target_policy jsonb NOT NULL DEFAULT '{}';
ALTER TABLE proxy_subject_policies ADD COLUMN IF NOT EXISTS reserved_proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE RESTRICT;
CREATE TABLE IF NOT EXISTS proxy_policy_group_reservations (
 policy_id bigint NOT NULL REFERENCES proxy_subject_policies(id) ON DELETE CASCADE,
 group_id bigint NOT NULL REFERENCES proxy_groups(id) ON DELETE RESTRICT, PRIMARY KEY(policy_id,group_id)
);
CREATE TABLE IF NOT EXISTS proxy_policy_groups (
 policy_id bigint NOT NULL REFERENCES proxy_subject_policies(id) ON DELETE CASCADE,
 group_id bigint NOT NULL REFERENCES proxy_groups(id) ON DELETE RESTRICT,
 PRIMARY KEY(policy_id,group_id)
);
CREATE TABLE IF NOT EXISTS proxy_subject_activity (
 token text PRIMARY KEY, policy_id bigint NOT NULL REFERENCES proxy_subject_policies(id) ON DELETE RESTRICT,
 revision bigint NOT NULL, proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE RESTRICT,
 backend_pid integer NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS proxy_subject_activity_policy_idx ON proxy_subject_activity(policy_id);
ALTER TABLE proxy_subject_activity ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'io';
CREATE TABLE IF NOT EXISTS proxy_failure_evidence (
 id text PRIMARY KEY, policy_id bigint NOT NULL REFERENCES proxy_subject_policies(id) ON DELETE CASCADE,
 revision bigint NOT NULL, proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE SET NULL,
 kind text NOT NULL CHECK(kind IN ('connection_failure','tunnel_failure','process_failure','health_confirmation')),
 created_at timestamptz NOT NULL DEFAULT now(), retry_after timestamptz
);
CREATE TABLE IF NOT EXISTS proxy_switch_requests (
 id text PRIMARY KEY, policy_id bigint NOT NULL REFERENCES proxy_subject_policies(id) ON DELETE CASCADE,
 base_revision bigint NOT NULL, authorization_revision bigint NOT NULL,
 state text NOT NULL CHECK(state IN ('pending','draining','applying','succeeded','failed','recovery_required')),
 from_proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE SET NULL,
 to_proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE SET NULL,
 target_snapshot jsonb NOT NULL DEFAULT '{}', group_revisions jsonb NOT NULL DEFAULT '{}',
 evidence_id text REFERENCES proxy_failure_evidence(id) ON DELETE RESTRICT,
 reason text NOT NULL, error_code text NOT NULL DEFAULT '', claim_token text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz, result_revision bigint
);
CREATE UNIQUE INDEX IF NOT EXISTS proxy_switch_single_intent_idx ON proxy_switch_requests(policy_id)
 WHERE state IN ('pending','draining','applying','recovery_required');
CREATE INDEX IF NOT EXISTS proxy_switch_subject_idx ON proxy_switch_requests(policy_id,created_at DESC);
