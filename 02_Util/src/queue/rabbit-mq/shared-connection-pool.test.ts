// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import { connect } from 'amqplib';
import type { Connection } from 'amqplib';
import { afterEach, expect, it, vi } from 'vitest';
import { clearPoolForUrl, getSharedChannel } from './shared-connection-pool.js';

vi.mock('amqplib', () => ({ connect: vi.fn() }));

const url = 'amqp://test';
afterEach(() => {
  clearPoolForUrl(url);
  vi.clearAllMocks();
  vi.useRealTimers();
});

it('keeps the shared connection usable when it runs out of channel IDs', async () => {
  const first = new EventEmitter();
  const next = new EventEmitter();
  const connection = Object.assign(new EventEmitter(), {
    close: vi.fn().mockResolvedValue(undefined),
    createChannel: vi
      .fn()
      .mockResolvedValueOnce(first)
      .mockRejectedValueOnce(new Error('No channels left to allocate'))
      .mockResolvedValueOnce(next),
  });
  vi.mocked(connect).mockResolvedValue(connection as unknown as Connection);

  expect(await getSharedChannel(url)).toBe(first);
  await expect(getSharedChannel(url)).rejects.toThrow('No channels left to allocate');
  expect(connection.close).not.toHaveBeenCalled();
  expect(await getSharedChannel(url)).toBe(next);
  expect(connect).toHaveBeenCalledOnce();
});

it('retires a stalled transport and uses a new connection after the channel-open timeout', async () => {
  vi.useFakeTimers();
  const stalled = Object.assign(new EventEmitter(), {
    close: vi.fn().mockResolvedValue(undefined),
    createChannel: vi.fn(() => new Promise(() => {})),
  });
  const next = new EventEmitter();
  const restored = Object.assign(new EventEmitter(), {
    close: vi.fn().mockResolvedValue(undefined),
    createChannel: vi.fn().mockResolvedValue(next),
  });
  vi.mocked(connect)
    .mockResolvedValueOnce(stalled as unknown as Connection)
    .mockResolvedValueOnce(restored as unknown as Connection);

  const opening = expect(getSharedChannel(url)).rejects.toThrow('channel creation timed out');
  await vi.advanceTimersByTimeAsync(10_000);
  await opening;
  expect(stalled.close).toHaveBeenCalledOnce();
  expect(await getSharedChannel(url)).toBe(next);
});
