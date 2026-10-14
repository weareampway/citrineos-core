// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ICache, SystemConfig } from '@citrineos/base';
import type { Channel } from 'amqplib';
import { Logger } from 'tslog';
import { RabbitMqReceiver } from './receiver.js';
import { rabbitMqClientsAreUnhealthy } from './connection-health.js';
import { getSharedChannel } from './shared-connection-pool.js';

vi.mock('./shared-connection-pool.js', () => ({ getSharedChannel: vi.fn() }));

class TestReceiver extends RabbitMqReceiver {
  reconnect() {
    return this._connectWithRetry();
  }
}

function channel() {
  return Object.assign(new EventEmitter(), {
    assertExchange: vi.fn().mockResolvedValue({}),
    assertQueue: vi.fn().mockResolvedValue({}),
    bindQueue: vi.fn().mockResolvedValue({}),
    consume: vi.fn().mockResolvedValue({}),
    close: vi.fn().mockResolvedValue(undefined),
  });
}

describe('RabbitMqReceiver recovery health', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-13T02:00:00Z'));
    vi.mocked(getSharedChannel).mockReset();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it('stays unhealthy across successful channel creations until all consumers recover', async () => {
    const config = {
      maxReconnectDelay: 30,
      util: { messageBroker: { amqp: { url: 'amqp://test', exchange: 'ocpp' } } },
    } as SystemConfig;
    const cache = {
      get: vi.fn().mockResolvedValue(null),
      set: vi.fn().mockResolvedValue(undefined),
      remove: vi.fn().mockResolvedValue(undefined),
    } as unknown as ICache;
    const receiver = new TestReceiver(config, new Logger({ minLevel: 7 }), undefined, cache);
    const initial = channel();
    vi.mocked(getSharedChannel).mockResolvedValueOnce(initial as unknown as Channel);
    await receiver.initConnection();
    await receiver.subscribe('Transactions');
    initial.emit('close');
    const disconnectedSince = receiver.disconnectedSince;

    const failedChannels: ReturnType<typeof channel>[] = [];
    vi.mocked(getSharedChannel).mockImplementation(async () => {
      const candidate = channel();
      candidate.bindQueue.mockRejectedValue(new Error('Queue binding failed'));
      failedChannels.push(candidate);
      return candidate as unknown as Channel;
    });
    await vi.advanceTimersByTimeAsync(121_000);

    expect(failedChannels.length).toBeGreaterThan(1);
    expect(receiver.disconnectedSince).toEqual(disconnectedSince);
    expect(rabbitMqClientsAreUnhealthy([receiver], new Date())).toBe(true);
    expect(
      failedChannels.slice(0, -1).every((candidate) => candidate.close.mock.calls.length === 1),
    ).toBe(true);

    const restored = channel();
    let finishConsume!: () => void;
    restored.consume.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishConsume = resolve;
        }),
    );
    vi.mocked(getSharedChannel).mockResolvedValue(restored as unknown as Channel);
    await vi.advanceTimersByTimeAsync(1000);
    const attempt = receiver.reconnect();
    expect(receiver.reconnect()).toBe(attempt);
    expect(rabbitMqClientsAreUnhealthy([receiver], new Date())).toBe(true);

    finishConsume();
    await attempt;
    expect(restored.consume).toHaveBeenCalledOnce();
    expect(receiver.disconnectedSince).toBeNull();
    expect(rabbitMqClientsAreUnhealthy([receiver], new Date())).toBe(false);
  });
});
