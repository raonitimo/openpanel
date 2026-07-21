import fs from 'node:fs';
import path from 'node:path';
import { TABLE_NAMES } from '../src/clickhouse/client';
import {
  modifyTTL,
  runClickhouseMigrationCommands,
} from '../src/clickhouse/migration';
import { getIsCluster, getIsDry, printBoxMessage } from './helpers';

/**
 * Migration 16 — 12-month retention TTL for `events` and `sessions` (finding F6).
 *
 * WHY: the raw `events` and `sessions` ClickHouse tables ship with no TTL, so
 * they grow unbounded. This applies a retention window of 12 months on the raw
 * rows (`created_at + INTERVAL 12 MONTH`). Long-range analytics are NOT lost:
 * they are served from the rollup materialized views (`dau_mv`,
 * `cohort_events_mv`, ...) which pre-aggregate history and are unaffected by
 * this TTL.
 *
 * IRREVERSIBLE: `ALTER TABLE ... MODIFY TTL` makes ClickHouse drop expired
 * parts. Rows older than 12 months are permanently deleted and cannot be
 * recovered.
 *
 * OPT-IN / NON-DESTRUCTIVE BY DEFAULT: because it destroys data, this migration
 * does nothing unless a human explicitly confirms it:
 *   - Without confirmation it only PRINTS the exact DDL it would run (and still
 *     writes the .sql preview), then returns without touching ClickHouse.
 *   - Pass `--apply-ttl` (argv) or set `CONFIRM_TTL=true` to actually execute.
 *   - The repo-wide `--dry` flag is honoured too and likewise skips execution.
 *
 * Merging the PR is therefore safe — it deletes no data. Applying the TTL is a
 * deliberate, manual step (run this migration by name with the confirm flag).
 */

const APPLY_TTL_FLAG = '--apply-ttl';
const RETENTION_TTL = 'created_at + INTERVAL 12 MONTH';

/**
 * Whether the operator has explicitly confirmed the destructive TTL apply.
 * Defaults to `false` so a normal migration run never drops data.
 */
export function getShouldApplyTtl(): boolean {
  return (
    process.argv.includes(APPLY_TTL_FLAG) || process.env.CONFIRM_TTL === 'true'
  );
}

/**
 * Pure builder for the retention DDL. Cluster-aware via `modifyTTL`.
 */
export function buildRetentionTtlSqls(isClustered: boolean): string[] {
  return [
    modifyTTL({
      tableName: TABLE_NAMES.events,
      isClustered,
      ttl: RETENTION_TTL,
    }),
    modifyTTL({
      tableName: TABLE_NAMES.sessions,
      isClustered,
      ttl: RETENTION_TTL,
    }),
  ];
}

export async function up() {
  const isClustered = getIsCluster();

  const sqls = buildRetentionTtlSqls(isClustered);

  fs.writeFileSync(
    path.join(import.meta.filename.replace('.ts', '.sql')),
    sqls
      .map((sql) =>
        sql
          .trim()
          .replace(/;$/, '')
          .replace(/\n{2,}/g, '\n')
          .concat(';'),
      )
      .join('\n\n---\n\n'),
  );

  const shouldApply = getShouldApplyTtl() && !getIsDry();

  if (!shouldApply) {
    printBoxMessage('⚠️  Retention TTL NOT applied (opt-in) ⚠️', [
      'This migration is destructive and irreversible: applying it drops',
      'events/sessions rows older than 12 months. Long-range analytics come',
      'from the rollup MVs (dau_mv, cohort_events_mv, ...), not raw rows.',
      '',
      `To apply, re-run this migration by name with ${APPLY_TTL_FLAG}`,
      'or CONFIRM_TTL=true. DDL that WOULD run:',
      '',
      ...sqls,
    ]);
    return;
  }

  await runClickhouseMigrationCommands(sqls);
}
