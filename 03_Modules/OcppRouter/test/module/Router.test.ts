// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { Call, ICache } from '@citrineos/base';
import { CacheNamespace, createIdentifier, ErrorCode, OCPPVersion } from '@citrineos/base';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MessageRouterImpl } from '../../src';

/**
 * Lock cache that wakes every waiter on a key when it is removed, like Redis keyspace
 * notifications do, so concurrent waiters race for the lock.
 */
class LockCache {
  values = new Map<string, string>();
  waiters = new Map<string, Array<(value: string | null) => void>>();

  exists = vi.fn(async () => false);

  setIfNotExist = vi.fn(async (key: string, value: string, namespace?: string) => {
    const namespaced = `${namespace}:${key}`;
    if (this.values.has(namespaced)) {
      return false;
    }
    this.values.set(namespaced, value);
    return true;
  });

  remove = vi.fn(async (key: string, namespace?: string) => {
    const namespaced = `${namespace}:${key}`;
    const removed = this.values.delete(namespaced);
    const waiters = this.waiters.get(namespaced) ?? [];
    this.waiters.delete(namespaced);
    waiters.forEach((wake) => wake(null));
    return removed;
  });

  onChange = vi.fn((key: string, waitSeconds: number, namespace?: string) => {
    const namespaced = `${namespace}:${key}`;
    return new Promise<string | null>((resolve) => {
      const timer = setTimeout(
        () => resolve(this.values.get(namespaced) ?? null),
        waitSeconds * 1000,
      );
      const waiters = this.waiters.get(namespaced) ?? [];
      waiters.push((value) => {
        clearTimeout(timer);
        resolve(value);
      });
      this.waiters.set(namespaced, waiters);
    });
  });
}

describe('MessageRouterImpl._onCall', () => {
  const identifier = createIdentifier(1, 'station');
  const heartbeat = (messageId: string): Call => [2, messageId, 'Heartbeat', {}];

  let cache: LockCache;
  let networkHook: ReturnType<typeof vi.fn>;
  let router: MessageRouterImpl;
  let routeCall: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    cache = new LockCache();
    networkHook = vi.fn(async () => undefined);
    router = new MessageRouterImpl(
      { maxCallLengthSeconds: 5, maxCachingSeconds: 10 } as any,
      cache as unknown as ICache,
      {} as any,
      { initConnection: vi.fn(async () => undefined) } as any,
      { dispatchMessageSent: vi.fn(async () => undefined) } as any,
      networkHook,
      new Logger<ILogObj>({ type: 'hidden' }),
      undefined,
      {} as any,
    );
    routeCall = vi
      .spyOn(router as any, '_routeCall')
      .mockImplementation(async () => ({ success: true }));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const sentCallErrors = () =>
    networkHook.mock.calls
      .map(([, raw]) => JSON.parse(raw as string))
      .filter((message) => message[0] === 4);

  it('takes the lock and routes the call when it is free', async () => {
    await router._onCall(identifier, heartbeat('A'), new Date(), OCPPVersion.OCPP1_6);

    expect(routeCall).toHaveBeenCalledTimes(1);
    expect(cache.values.get(`${CacheNamespace.Transactions}:${identifier}`)).toBe('Heartbeat:A');
  });

  it('lets every waiter through one after another when the lock is released', async () => {
    await router._onCall(identifier, heartbeat('A'), new Date(), OCPPVersion.OCPP1_6);
    const second = router._onCall(identifier, heartbeat('B'), new Date(), OCPPVersion.OCPP1_6);
    const third = router._onCall(identifier, heartbeat('C'), new Date(), OCPPVersion.OCPP1_6);
    await vi.advanceTimersByTimeAsync(0);
    expect(routeCall).toHaveBeenCalledTimes(1);

    // A answered: both waiters wake, one gets the lock, the other waits again.
    await cache.remove(identifier, CacheNamespace.Transactions);
    await vi.advanceTimersByTimeAsync(0);
    expect(routeCall).toHaveBeenCalledTimes(2);
    expect(sentCallErrors()).toEqual([]);

    await cache.remove(identifier, CacheNamespace.Transactions);
    await vi.advanceTimersByTimeAsync(0);
    await Promise.all([second, third]);

    expect(routeCall).toHaveBeenCalledTimes(3);
    expect(sentCallErrors()).toEqual([]);
  });

  it('sends a CallError once the lock is still held at the deadline', async () => {
    await router._onCall(identifier, heartbeat('A'), new Date(), OCPPVersion.OCPP1_6);
    const second = router._onCall(identifier, heartbeat('B'), new Date(), OCPPVersion.OCPP1_6);

    await vi.advanceTimersByTimeAsync(5000);
    await second;

    expect(routeCall).toHaveBeenCalledTimes(1);
    expect(sentCallErrors()).toEqual([
      [4, 'B', ErrorCode.RpcFrameworkError, 'Call already in progress', {}],
    ]);
  });

  it('routes an OCPP 1.6 SecurityEventNotification like any other call', async () => {
    const call: Call = [
      2,
      'S',
      'SecurityEventNotification',
      { type: 'SecurityLogWasCleared', timestamp: '2026-10-28T12:00:00.000Z' },
    ];

    await router._onCall(identifier, call, new Date(), OCPPVersion.OCPP1_6);

    expect(routeCall).toHaveBeenCalledTimes(1);
    expect(sentCallErrors()).toEqual([]);
  });
});
