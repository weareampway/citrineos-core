// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RedisCache } from '../../src/cache/redis.js';

type Listener = (event: string) => void;

class FakeRedisClient {
  options: { database?: number } = {};
  store = new Map<string, string>();
  listeners = new Map<string, Set<Listener>>();
  duplicates: FakeRedisClient[] = [];
  // Resolves pending SUBSCRIBE replies; tests release them to control when a subscription is live.
  pendingSubscribes: Array<() => void> = [];
  holdSubscribes = false;

  on = vi.fn(() => this);
  connect = vi.fn(() => Promise.resolve());
  get = vi.fn((key: string) => Promise.resolve(this.store.get(key) ?? null));

  duplicate = vi.fn(() => {
    const duplicate = new FakeRedisClient();
    duplicate.options = this.options;
    duplicate.holdSubscribes = this.holdSubscribes;
    this.duplicates.push(duplicate);
    return duplicate;
  });

  subscribe = vi.fn((channel: string, listener: Listener) => {
    const register = () => {
      const set = this.listeners.get(channel) ?? new Set<Listener>();
      set.add(listener);
      this.listeners.set(channel, set);
    };
    if (!this.holdSubscribes) {
      register();
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.pendingSubscribes.push(() => {
        register();
        resolve();
      });
    });
  });

  unsubscribe = vi.fn((channel: string, listener: Listener) => {
    this.listeners.get(channel)?.delete(listener);
    return Promise.resolve();
  });

  emit(channel: string, event: string) {
    this.listeners.get(channel)?.forEach((listener) => listener(event));
  }

  listenerCount(channel: string) {
    return this.listeners.get(channel)?.size ?? 0;
  }
}

let client: FakeRedisClient;

vi.mock('redis', () => ({
  createClient: () => client,
}));

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('RedisCache.onChange', () => {
  const channel = '__keyspace@0__:Transactions:1:station';
  const redisKey = 'Transactions:1:station';

  beforeEach(() => {
    client = new FakeRedisClient();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('resolves null when the key is deleted', async () => {
    client.store.set(redisKey, 'Heartbeat:1');
    const cache = new RedisCache();

    const result = cache.onChange('1:station', 5, 'Transactions');
    await flush();
    client.store.delete(redisKey);
    client.duplicates[0].emit(channel, 'del');

    await expect(result).resolves.toBeNull();
  });

  it('resolves null when the key expires, but not when only a TTL is set', async () => {
    vi.useFakeTimers();
    client.store.set(redisKey, 'Heartbeat:1');
    const cache = new RedisCache();
    let settled = false;

    const result = cache.onChange('1:station', 5, 'Transactions').then((value) => {
      settled = true;
      return value;
    });
    await vi.advanceTimersByTimeAsync(0);
    client.duplicates[0].emit(channel, 'expire');
    await vi.advanceTimersByTimeAsync(0);
    expect(settled).toBe(false);

    client.store.delete(redisKey);
    client.duplicates[0].emit(channel, 'expired');

    await expect(result).resolves.toBeNull();
  });

  it('resolves with the new value when the key is set', async () => {
    const cache = new RedisCache();

    const result = cache.onChange('correlation', 5, 'station');
    await flush();
    client.store.set('station:correlation', '{"status":"Accepted"}');
    client.duplicates[0].emit('__keyspace@0__:station:correlation', 'set');

    await expect(result).resolves.toBe('{"status":"Accepted"}');
  });

  it('notices a change made before the subscription was live', async () => {
    client.store.set(redisKey, 'Heartbeat:1');
    client.holdSubscribes = true;
    const cache = new RedisCache();

    const result = cache.onChange('1:station', 5, 'Transactions');
    await flush();
    // The lock is released while SUBSCRIBE is still in flight; the 'del' is never delivered.
    client.store.delete(redisKey);
    client.duplicates[0].pendingSubscribes.forEach((release) => release());

    await expect(result).resolves.toBeNull();
  });

  it('keeps waiting when the key is unchanged once the subscription is live', async () => {
    vi.useFakeTimers();
    client.store.set(redisKey, 'Heartbeat:1');
    const cache = new RedisCache();
    let settled = false;

    const result = cache.onChange('1:station', 5, 'Transactions').then((value) => {
      settled = true;
      return value;
    });
    await vi.advanceTimersByTimeAsync(4000);
    expect(settled).toBe(false);

    await vi.advanceTimersByTimeAsync(1000);
    await expect(result).resolves.toBe('Heartbeat:1');
  });

  it('shares one subscriber connection and unsubscribes after each wait', async () => {
    client.store.set(redisKey, 'Heartbeat:1');
    const cache = new RedisCache();

    const first = cache.onChange('1:station', 5, 'Transactions');
    const second = cache.onChange('1:station', 5, 'Transactions');
    await flush();
    expect(client.duplicate).toHaveBeenCalledTimes(1);
    expect(client.duplicates[0].listenerCount(channel)).toBe(2);

    client.store.delete(redisKey);
    client.duplicates[0].emit(channel, 'del');
    await expect(Promise.all([first, second])).resolves.toEqual([null, null]);
    await flush();

    expect(client.duplicates[0].listenerCount(channel)).toBe(0);
    expect(client.duplicates[0].connect).toHaveBeenCalledTimes(1);
  });

  it('subscribes on the keyspace channel of the selected database', async () => {
    client.options = { database: 2 };
    const cache = new RedisCache();

    void cache.onChange('1:station', 5, 'Transactions');
    await flush();

    expect(client.duplicates[0].subscribe).toHaveBeenCalledWith(
      '__keyspace@2__:Transactions:1:station',
      expect.any(Function),
    );
  });
});
