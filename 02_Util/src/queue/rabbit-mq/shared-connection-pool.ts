// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import * as amqplib from 'amqplib';

interface PoolEntry {
  connection: amqplib.Connection;
}

const poolsByUrl = new Map<
  string,
  { entry: PoolEntry | null; connectPromise: Promise<PoolEntry> | null }
>();

function getPoolForUrl(url: string) {
  let pool = poolsByUrl.get(url);
  if (!pool) {
    pool = { entry: null, connectPromise: null };
    poolsByUrl.set(url, pool);
  }
  return pool;
}

export async function getSharedChannel(url: string): Promise<amqplib.Channel> {
  const pool = getPoolForUrl(url);

  const ensureConnection = async (): Promise<PoolEntry> => {
    if (pool.entry?.connection) {
      return pool.entry;
    }
    if (pool.connectPromise) {
      return pool.connectPromise;
    }
    pool.connectPromise = (async () => {
      try {
        const connection = await amqplib.connect(url);
        connection.on('close', () => {
          pool.entry = null;
          pool.connectPromise = null;
        });
        connection.on('error', () => {
          pool.entry = null;
          pool.connectPromise = null;
        });
        pool.entry = { connection };
        pool.connectPromise = null;
        return pool.entry;
      } catch (err) {
        pool.connectPromise = null;
        throw err;
      }
    })();
    return pool.connectPromise;
  };

  const entry = await ensureConnection();
  const channel = await entry.connection.createChannel();
  return channel;
}

export function clearPoolForUrl(url: string): void {
  const pool = poolsByUrl.get(url);
  if (pool?.entry?.connection) {
    pool.entry.connection.close().catch(() => {});
    pool.entry = null;
    pool.connectPromise = null;
  }
}
