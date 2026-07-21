import { createHash } from 'node:crypto';
import { getSafeJson } from '@openpanel/json';
import { getRedisCache, publishEvent } from '@openpanel/redis';
import { ch, chQuery } from '../clickhouse/client';
import type { IClickhouseEvent } from '../services/event.service';
import { BaseBuffer } from './base-buffer';

export class EventBuffer extends BaseBuffer {
  private batchSize = process.env.EVENT_BUFFER_BATCH_SIZE
    ? Number.parseInt(process.env.EVENT_BUFFER_BATCH_SIZE, 10)
    : 4000;
  private chunkSize = process.env.EVENT_BUFFER_CHUNK_SIZE
    ? Number.parseInt(process.env.EVENT_BUFFER_CHUNK_SIZE, 10)
    : 1000;

  private microBatchIntervalMs = process.env.EVENT_BUFFER_MICRO_BATCH_MS
    ? Number.parseInt(process.env.EVENT_BUFFER_MICRO_BATCH_MS, 10)
    : 10;
  private microBatchMaxSize = process.env.EVENT_BUFFER_MICRO_BATCH_SIZE
    ? Number.parseInt(process.env.EVENT_BUFFER_MICRO_BATCH_SIZE, 10)
    : 100;

  private pendingEvents: IClickhouseEvent[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private isFlushing = false;
  /** Tracks consecutive flush failures for observability; reset on success. */
  private flushRetryCount = 0;

  private queueKey = 'event_buffer:queue';
  protected bufferCounterKey = 'event_buffer:total_count';

  constructor() {
    super({
      name: 'event',
      onFlush: async () => {
        await this.processBuffer();
      },
    });
  }

  bulkAdd(events: IClickhouseEvent[]) {
    for (const event of events) {
      this.add(event);
    }
  }

  add(event: IClickhouseEvent) {
    this.pendingEvents.push(event);

    if (this.pendingEvents.length >= this.microBatchMaxSize) {
      this.flushLocalBuffer();
      return;
    }

    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        this.flushLocalBuffer();
      }, this.microBatchIntervalMs);
    }
  }

  public async flush() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flushLocalBuffer();
  }

  private async flushLocalBuffer() {
    if (this.isFlushing || this.pendingEvents.length === 0) {
      return;
    }

    this.isFlushing = true;

    const eventsToFlush = this.pendingEvents;
    this.pendingEvents = [];

    try {
      const redis = getRedisCache();
      const multi = redis.multi();

      for (const event of eventsToFlush) {
        multi.rpush(this.queueKey, JSON.stringify(event));
      }
      multi.incrby(this.bufferCounterKey, eventsToFlush.length);

      await multi.exec();

      this.flushRetryCount = 0;
    } catch (error) {
      // Re-queue failed events at the front to preserve order and avoid data loss
      this.pendingEvents = eventsToFlush.concat(this.pendingEvents);

      this.flushRetryCount += 1;
      this.logger.warn(
        'Failed to flush local buffer to Redis; events re-queued',
        {
          error,
          eventCount: eventsToFlush.length,
          flushRetryCount: this.flushRetryCount,
        }
      );
    } finally {
      this.isFlushing = false;
      // Events may have accumulated while we were flushing; schedule another flush if needed
      if (this.pendingEvents.length > 0 && !this.flushTimer) {
        this.flushTimer = setTimeout(() => {
          this.flushTimer = null;
          this.flushLocalBuffer();
        }, this.microBatchIntervalMs);
      }
    }
  }

  /**
   * Deterministic idempotency token for a chunk: the same chunk content on a
   * retry produces the same token, so ClickHouse rejects the duplicate insert.
   */
  private deduplicationToken(events: IClickhouseEvent[]): string {
    return createHash('sha256').update(JSON.stringify(events)).digest('hex');
  }

  async processBuffer() {
    const redis = getRedisCache();

    try {
      const queueEvents = await redis.lrange(
        this.queueKey,
        0,
        this.batchSize - 1
      );

      if (queueEvents.length === 0) {
        this.logger.debug('No events to process');
        return;
      }

      const rawChunks = this.chunks(queueEvents, this.chunkSize);

      this.logger.info('Inserting events into ClickHouse', {
        totalEvents: queueEvents.length,
        chunks: rawChunks.length,
      });

      // Process chunks in queue order. Each chunk is trimmed from the front of
      // the queue only AFTER it is safely inserted. If a chunk insert throws,
      // we stop: already-committed chunks stay trimmed and the untrimmed
      // remainder is retried next cycle — committed chunks are never replayed
      // (which would duplicate rows, since `events` has no dedup key).
      let eventsProcessed = 0;
      for (const rawChunk of rawChunks) {
        const chunkEvents: IClickhouseEvent[] = [];
        for (const eventStr of rawChunk) {
          const event = getSafeJson<IClickhouseEvent>(eventStr);
          if (event) {
            if (!Array.isArray(event.groups)) {
              event.groups = [];
            }
            chunkEvents.push(event);
          }
        }

        chunkEvents.sort(
          (a, b) =>
            new Date(a.created_at || 0).getTime() -
            new Date(b.created_at || 0).getTime()
        );

        if (chunkEvents.length > 0) {
          await ch.insert({
            table: 'events',
            values: chunkEvents,
            format: 'JSONEachRow',
            clickhouse_settings: {
              insert_deduplication_token: this.deduplicationToken(chunkEvents),
            },
          });
        }

        // Commit this chunk: remove exactly the raw entries we just processed.
        await redis
          .multi()
          .ltrim(this.queueKey, rawChunk.length, -1)
          .decrby(this.bufferCounterKey, rawChunk.length)
          .exec();

        if (chunkEvents.length > 0) {
          const countByProject = new Map<string, number>();
          for (const event of chunkEvents) {
            countByProject.set(
              event.project_id,
              (countByProject.get(event.project_id) ?? 0) + 1
            );
          }
          for (const [projectId, count] of countByProject) {
            publishEvent('events', 'batch', { projectId, count });
          }
          eventsProcessed += chunkEvents.length;
        }
      }

      this.logger.info('Processed events from Redis buffer', {
        batchSize: this.batchSize,
        eventsProcessed,
      });
    } catch (error) {
      this.logger.error('Error processing Redis buffer', { error });
    }
  }

  public getBufferSize() {
    return this.getBufferSizeWithCounter(async () => {
      const redis = getRedisCache();
      return await redis.llen(this.queueKey);
    });
  }

  public async getActiveVisitorCount(projectId: string): Promise<number> {
    const rows = await chQuery<{ count: number }>(
      `SELECT uniq(profile_id) AS count
       FROM events
       WHERE project_id = '${projectId}'
         AND profile_id != ''
         AND created_at >= now() - INTERVAL 5 MINUTE`
    );
    return rows[0]?.count ?? 0;
  }
}
