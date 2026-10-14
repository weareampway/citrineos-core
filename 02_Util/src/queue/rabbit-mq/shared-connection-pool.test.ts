// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as amqplib from 'amqplib';
import { clearPoolForUrl, getSharedChannel } from './shared-connection-pool.js';

vi.mock('amqplib', () => ({ connect: vi.fn() }));
const url = 'amqp://synthetic';
function connection() {
  return Object.assign(new EventEmitter(), {
    close: vi.fn().mockResolvedValue(undefined),
    createChannel: vi.fn().mockResolvedValue({}),
  });
}
function returns(value: ReturnType<typeof connection>) {
  vi.mocked(amqplib.connect).mockResolvedValueOnce(value as unknown as amqplib.Connection);
}

beforeEach(() => {
  vi.mocked(amqplib.connect).mockReset();
});
afterEach(() => {
  clearPoolForUrl(url);
  vi.useRealTimers();
});

describe('shared RabbitMQ transport recovery', () => {
  it('opens one transport for concurrent channels', async () => {
    const current = connection();
    returns(current);
    await Promise.all([getSharedChannel(url), getSharedChannel(url), getSharedChannel(url)]);
    expect(amqplib.connect).toHaveBeenCalledOnce();
    expect(amqplib.connect).toHaveBeenCalledWith(url, { timeout: 10_000 });
    expect(current.createChannel).toHaveBeenCalledTimes(3);
  });

  it('a retired connection cannot invalidate its replacement with late error/close events', async () => {
    const old = connection();
    returns(old);
    await getSharedChannel(url);
    old.emit('error', new Error('transport failed'));
    const replacement = connection();
    returns(replacement);
    await getSharedChannel(url);
    old.emit('close');
    old.emit('error', new Error('late error'));
    await getSharedChannel(url);
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
    expect(replacement.createChannel).toHaveBeenCalledTimes(2);
  });

  it('a late close cannot clear a pending replacement and spawn extra transports', async () => {
    const old = connection();
    returns(old);
    await getSharedChannel(url);
    old.emit('error', new Error('failed'));
    let finish!: (value: amqplib.Connection) => void;
    vi.mocked(amqplib.connect).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const first = getSharedChannel(url);
    old.emit('close');
    const second = getSharedChannel(url);
    const replacement = connection();
    finish(replacement as unknown as amqplib.Connection);
    await Promise.all([first, second]);
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
  });

  it('retries after a rejected handshake', async () => {
    vi.mocked(amqplib.connect).mockRejectedValueOnce(new Error('handshake failed'));
    await expect(getSharedChannel(url)).rejects.toThrow('handshake failed');
    const current = connection();
    returns(current);
    await getSharedChannel(url);
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
  });

  it('does not reuse a transport that cannot create a channel', async () => {
    const old = connection();
    old.createChannel.mockRejectedValue(new Error('closed'));
    returns(old);
    await expect(getSharedChannel(url)).rejects.toThrow('closed');
    const next = connection();
    returns(next);
    await getSharedChannel(url);
    expect(old.close).toHaveBeenCalledOnce();
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
  });

  it('bounds a half-open channel handshake and obtains a fresh transport', async () => {
    vi.useFakeTimers();
    const old = connection();
    old.createChannel.mockImplementation(() => new Promise(() => {}));
    returns(old);
    const failed = expect(getSharedChannel(url)).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(10_000);
    await failed;
    const next = connection();
    returns(next);
    await getSharedChannel(url);
    expect(old.close).toHaveBeenCalledOnce();
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
  });

  it('clearing while connecting closes the late result without replacing the new pool', async () => {
    let finish!: (value: amqplib.Connection) => void;
    vi.mocked(amqplib.connect).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const failed = expect(getSharedChannel(url)).rejects.toThrow('cleared');
    clearPoolForUrl(url);
    const next = connection();
    returns(next);
    await getSharedChannel(url);
    const old = connection();
    finish(old as unknown as amqplib.Connection);
    await failed;
    await getSharedChannel(url);
    expect(old.close).toHaveBeenCalledOnce();
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
  });
});
