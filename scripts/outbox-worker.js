// ============================================================
// KIS Outbox Worker
// KIS-ADR-003 (Rev.1, 2026-09-10) 準拠
// KIS-ADR-004 (2026-09-15) 反映：kis_solution_selections の
//   evaluation_reason 列を廃止、selection_reason に一本化
// 未来図Lab工房／長谷川浩一郎
//
// kis_sync_outbox の pending 行をポーリングし、Neo4j Aura へ
// Cypher MERGE で反映する。GitHub Actions cron から5分おきに
// 起動される想定（常駐プロセスではない）。
// ============================================================

import pg from 'pg';
import neo4j from 'neo4j-driver';

const MAX_RETRY = 5; // ADR-003本文「暫定5回」

// 指数バックオフ：1回目失敗→1分後、2回目→2分後、3回目→4分後...
// 上限30分でキャップ（cronの5分間隔に対して極端に長くしすぎない）
function nextRetryDelayMinutes(retryCount) {
  return Math.min(2 ** (retryCount - 1), 30);
}

// ============================================================
// テーブルごとのCypher組み立て（ADR-003 Rev.1：対象7テーブル）
// ============================================================
function buildCypher(row) {
  const p = row.payload;

  switch (row.target_table) {
    case 'kis_sessions':
      return {
        query: `
          MERGE (s:Session {id: $id})
          SET s.started_at = datetime($started_at),
              s.ended_at = CASE WHEN $ended_at IS NULL THEN null ELSE datetime($ended_at) END,
              s.mode = $mode,
              s.level = $level,
              s.source_check = $source_check,
              s.commit_policy = $commit_policy
        `,
        params: p,
      };

    case 'kis_cognitive_states':
      return {
        query: `
          MERGE (cs:CognitiveState {id: $id})
          SET cs.session_id = $session_id, cs.stage = $stage, cs.text = $text,
              cs.committed = $committed, cs.created_at = datetime($created_at),
              cs.lineage_id = $lineage_id, cs.revision_no = $revision_no
          WITH cs
          MERGE (s:Session {id: $session_id})
          MERGE (cs)-[:BELONGS_TO]->(s)
        `,
        params: p,
      };

    case 'kis_gate_events':
      return {
        query: `
          MERGE (ge:GateEvent {id: $id})
          SET ge.session_id = $session_id, ge.gate_type = $gate_type,
              ge.from_state_id = $from_state_id, ge.to_state_id = $to_state_id,
              ge.judgment = $judgment, ge.passed = $passed, ge.reason = $reason,
              ge.created_at = datetime($created_at)
          WITH ge
          MERGE (from:CognitiveState {id: $from_state_id})
          MERGE (from)-[:GATE_CHECKED]->(ge)
        `,
        params: p,
      };

    case 'kis_commit_locks':
      return {
        query: `
          MERGE (cl:CommitLock {id: $id})
          SET cl.state_id = $state_id, cl.committed_at = datetime($committed_at),
              cl.reopened = $reopened, cl.reopen_reason = $reopen_reason,
              cl.reopen_approved = $reopen_approved,
              cl.reopened_at = CASE WHEN $reopened_at IS NULL THEN null ELSE datetime($reopened_at) END
          WITH cl
          MERGE (cs:CognitiveState {id: $state_id})
          MERGE (cs)-[:HAS_COMMIT]->(cl)
        `,
        params: p,
      };

    case 'kis_state_transitions_v2':
      return {
        query: `
          MERGE (from:CognitiveState {id: $from_state_id})
          MERGE (to:CognitiveState {id: $to_state_id})
          MERGE (from)-[r:NEXT {transition_id: $id}]->(to)
          SET r.n = $n, r.is_inquiry_step = $is_inquiry_step, r.note = $note,
              r.created_at = datetime($created_at)
        `,
        params: p,
      };

    case 'kis_cognitive_state_revisions':
      return {
        query: `
          MERGE (new:CognitiveState {id: $new_state_id})
          MERGE (old:CognitiveState {id: $revises_state_id})
          MERGE (new)-[r:REVISES {revision_id: $id}]->(old)
          SET r.created_at = datetime($created_at)
        `,
        params: p,
      };

    case 'kis_solution_selections':
      // KIS-ADR-004: evaluation_reason は selection_reason と機能重複のため廃止。
      // selection_reason に一本化する。
      return {
        query: `
          MERGE (selected:CognitiveState {id: $selected_state_id})
          MERGE (candidate:CognitiveState {id: $candidate_state_id})
          MERGE (selected)-[r:CHOSEN_FROM {selection_id: $id}]->(candidate)
          SET r.selection_reason = $selection_reason,
              r.created_at = datetime($created_at)
        `,
        params: p,
      };

    default:
      throw new Error(`未対応のtarget_table: ${row.target_table}`);
  }
}

// ============================================================
// メイン処理
// ============================================================
async function main() {
  const pgClient = new pg.Client({
    connectionString: process.env.NEON_CONNECTION_STRING,
    connectionTimeoutMillis: 15000, // Neonのコールドスタート（オートサスペンド復帰）を考慮した余裕
  });
  const neo4jDriver = neo4j.driver(
    process.env.NEO4J_URI,
    neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
  );

  await pgClient.connect();
  const session = neo4jDriver.session();

  let synced = 0, retried = 0, deadFailed = 0;

  const BATCH_LIMIT = 200; // 1回の実行で処理する上限（ロック保持時間を有界にする）

  try {
    // ワークフローの並行実行（cronと手動再実行の重複等）で同じ行を
    // 二重処理しないよう、トランザクション内でFOR UPDATE SKIP LOCKEDを使う。
    // 他の実行が既にロック中の行はスキップされ、次回実行に回る。
    await pgClient.query('BEGIN');

    const { rows } = await pgClient.query(`
      SELECT * FROM kis_sync_outbox
      WHERE status = 'pending'
        AND (next_retry_at IS NULL OR next_retry_at <= now())
      ORDER BY created_at ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    `, [BATCH_LIMIT]);

    console.log(`処理対象: ${rows.length}件`);

    for (const row of rows) {
      try {
        const { query, params } = buildCypher(row);
        await session.run(query, params);

        await pgClient.query(`
          UPDATE kis_sync_outbox
          SET status = 'synced', synced_at = now()
          WHERE id = $1
        `, [row.id]);
        synced++;

      } catch (err) {
        const nextRetryCount = row.retry_count + 1;

        if (nextRetryCount >= MAX_RETRY) {
          await pgClient.query(`
            UPDATE kis_sync_outbox
            SET status = 'failed', retry_count = $1, last_error = $2
            WHERE id = $3
          `, [nextRetryCount, String(err.message), row.id]);
          deadFailed++;
          console.error(`[失敗・諦め] id=${row.id} table=${row.target_table}: ${err.message}`);

        } else {
          const delayMin = nextRetryDelayMinutes(nextRetryCount);
          await pgClient.query(`
            UPDATE kis_sync_outbox
            SET status = 'pending',
                retry_count = $1,
                last_error = $2,
                next_retry_at = now() + ($3 || ' minutes')::interval
            WHERE id = $4
          `, [nextRetryCount, String(err.message), delayMin, row.id]);
          retried++;
          console.warn(`[リトライ${nextRetryCount}/${MAX_RETRY}] id=${row.id} table=${row.target_table}: ${err.message} (次回 ${delayMin}分後)`);
        }
      }
    }

    await pgClient.query('COMMIT');
    console.log(`完了: synced=${synced} retried=${retried} failed=${deadFailed} total=${rows.length}`);

    // KIS-ADR-006 (2026-09-15): 最終失敗が1件でもあればジョブを失敗扱いにし、
    // GitHub Actions標準のメール通知（失敗したワークフローの実行者への通知）を
    // 発火させる。新規サービス・シークレットを増やさない方針のため。
    if (deadFailed > 0) {
      console.error(`△要確認: ${deadFailed}件が最終的にstatus='failed'になりました。Neonのkis_sync_outboxを確認してください。`);
      process.exitCode = 1;
    }

  } catch (err) {
    await pgClient.query('ROLLBACK').catch(() => {});
    throw err;

  } finally {
    await session.close();
    await neo4jDriver.close();
    await pgClient.end();
  }
}

main().catch(err => {
  console.error('outboxワーカー致命的エラー:', err);
  process.exit(1);
});
