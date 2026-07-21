import { afterEach, describe, expect, it } from 'vitest';
import {
  buildRetentionTtlSqls,
  getShouldApplyTtl,
} from './16-events-sessions-retention-ttl';

describe('16-events-sessions-retention-ttl', () => {
  const originalArgv = process.argv;
  const originalConfirm = process.env.CONFIRM_TTL;

  afterEach(() => {
    process.argv = originalArgv;
    if (originalConfirm === undefined) {
      delete process.env.CONFIRM_TTL;
    } else {
      process.env.CONFIRM_TTL = originalConfirm;
    }
  });

  it('emits a 12-month TTL DDL for both events and sessions (non-clustered)', () => {
    const sqls = buildRetentionTtlSqls(false);
    expect(sqls).toHaveLength(2);

    const events = sqls.find((sql) => /ALTER TABLE events\b/.test(sql));
    const sessions = sqls.find((sql) => /ALTER TABLE sessions\b/.test(sql));

    expect(events).toContain('created_at + INTERVAL 12 MONTH');
    expect(sessions).toContain('created_at + INTERVAL 12 MONTH');
  });

  it('targets the replicated tables on cluster and keeps the 12-month window', () => {
    const sqls = buildRetentionTtlSqls(true);

    expect(sqls.every((sql) => sql.includes("ON CLUSTER '{cluster}'"))).toBe(
      true,
    );
    expect(
      sqls.every((sql) => sql.includes('created_at + INTERVAL 12 MONTH')),
    ).toBe(true);
    expect(sqls.some((sql) => sql.includes('events_replicated'))).toBe(true);
    expect(sqls.some((sql) => sql.includes('sessions_replicated'))).toBe(true);
  });

  it('does not apply the TTL unless explicitly confirmed', () => {
    process.argv = ['node', 'migrate.ts'];
    delete process.env.CONFIRM_TTL;
    expect(getShouldApplyTtl()).toBe(false);

    process.argv = ['node', 'migrate.ts', '--apply-ttl'];
    expect(getShouldApplyTtl()).toBe(true);

    process.argv = ['node', 'migrate.ts'];
    process.env.CONFIRM_TTL = 'true';
    expect(getShouldApplyTtl()).toBe(true);
  });
});
