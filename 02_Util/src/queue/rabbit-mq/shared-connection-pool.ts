// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import * as amqplib from 'amqplib';
import type { ILogObj, Logger } from 'tslog';

interface PoolEntry {
  connection: amqplib.Connection;
}
interface Pool {
  entry: PoolEntry | null;
  connectPromise: Promise<PoolEntry> | null;
}

const poolsByUrl = new Map<string, Pool>();

function getPoolForUrl(url: string): Pool {
  let pool = poolsByUrl.get(url);
  if (!pool) {
    pool = { entry: null, connectPromise: null };
    poolsByUrl.set(url, pool);
  }
  return pool;
}

// Log transport diagnostics without logging the AMQP URL, credentials or payloads.
function errorDetails(error: unknown) {
  const err = error as { code?: unknown; errno?: unknown; syscall?: unknown } | undefined;
  return { code: err?.code, errno: err?.errno, syscall: err?.syscall };
}

export async function getSharedChannel(
  url: string,
  logger?: Logger<ILogObj>,
): Promise<amqplib.Channel> {
  const pool = getPoolForUrl(url);
  if (!pool.entry && !pool.connectPromise) {
    const attempt = amqplib.connect(url, { timeout: 10_000 }).then(async (connection) => {
      const entry = { connection };
      connection.on('error', (err) => {
        logger?.error('Shared RabbitMQ connection error', errorDetails(err));
        // A late event from a retired connection must never invalidate its successor.
        if (pool.entry === entry) pool.entry = null;
      });
      connection.on('close', (err) => {
        logger?.warn('Shared RabbitMQ connection closed', errorDetails(err));
        if (pool.entry === entry) pool.entry = null;
      });
      if (poolsByUrl.get(url) !== pool) {
        await connection.close().catch(() => {});
        throw new Error('RabbitMQ connection pool was cleared while connecting');
      }
      pool.entry = entry;
      return entry;
    });
    pool.connectPromise = attempt;
    // Use both handlers: an ignored finally() would create an unhandled rejection.
    const release = () => {
      if (pool.connectPromise === attempt) pool.connectPromise = null;
    };
    void attempt.then(release, release);
  }
  const entry = pool.entry ?? (await pool.connectPromise!);
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      entry.connection.createChannel().then((channel) => {
        if (pool.entry !== entry || poolsByUrl.get(url) !== pool) {
          channel.on('error', () => {});
          void channel.close().catch(() => {});
          throw new Error('RabbitMQ transport retired while opening a channel');
        }
        return channel;
      }),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error('RabbitMQ channel creation timed out')),
          10_000,
        );
      }),
    ]);
  } catch (err) {
    // A half-open transport may never emit error/close. Do not reuse it forever.
    if (pool.entry === entry) pool.entry = null;
    void entry.connection.close().catch(() => {});
    throw err;
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

export function clearPoolForUrl(url: string): void {
  const pool = poolsByUrl.get(url);
  poolsByUrl.delete(url);
  void pool?.entry?.connection.close().catch(() => {});
}
