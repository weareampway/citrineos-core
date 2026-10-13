// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import type { CircuitBreaker, SystemConfig } from '@citrineos/base';
import type { Channel } from 'amqplib';
import { type ILogObj, Logger } from 'tslog';
import { RabbitMqSender } from './sender.js';
import { getSharedChannel } from './shared-connection-pool.js';

vi.mock('./shared-connection-pool.js', () => ({ getSharedChannel: vi.fn() }));

class TestSender extends RabbitMqSender {
  reconnect() {
    return this._connectWithRetry();
  }
}

function channel() {
  return Object.assign(new EventEmitter(), {
    close: vi.fn().mockResolvedValue(undefined),
  });
}

function breaker(): CircuitBreaker {
  return {
    state: 'OPEN',
    onStateChange: vi.fn(),
    triggerSuccess: vi.fn(),
    triggerFailure: vi.fn(),
  } as unknown as CircuitBreaker;
}

const config = {
  maxReconnectDelay: 30,
  util: { messageBroker: { amqp: { url: 'amqp://test', exchange: 'ocpp' } } },
} as SystemConfig;

describe('RabbitMqSender channel retirement', () => {
  it('closes the channel it disconnects from and waits for that close before opening another', async () => {
    const logger = new Logger<ILogObj>({ minLevel: 7 });
    const initial = channel();
    let finishClose!: () => void;
    initial.close.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishClose = resolve;
        }),
    );
    vi.mocked(getSharedChannel).mockResolvedValueOnce(initial as unknown as Channel);
    const sender = new TestSender(config, logger, breaker());
    await vi.waitFor(() => expect(initial.listenerCount('close')).toBe(1));

    initial.emit('close');
    const replacement = channel();
    vi.mocked(getSharedChannel).mockResolvedValueOnce(replacement as unknown as Channel);
    let reconnected = false;
    const attempt = sender.reconnect().then(() => {
      reconnected = true;
    });
    await Promise.resolve();
    expect(reconnected).toBe(false);
    expect(getSharedChannel).toHaveBeenCalledTimes(1);

    finishClose();
    await attempt;
    expect(initial.close).toHaveBeenCalledOnce();
    expect(replacement.listenerCount('close')).toBe(1);
  });

  it('reports one connection loss when the same channel reports two errors', async () => {
    const logger = new Logger<ILogObj>({ minLevel: 7 });
    const circuit = breaker();
    const initial = channel();
    vi.mocked(getSharedChannel).mockResolvedValueOnce(initial as unknown as Channel);
    new TestSender(config, logger, circuit);
    await vi.waitFor(() => expect(initial.listenerCount('error')).toBe(1));

    const onError = initial.listeners('error')[0] as (err: Error) => void;
    onError(new Error('down'));
    onError(new Error('still down'));

    expect(circuit.triggerFailure).toHaveBeenCalledTimes(1);
    expect(initial.close).toHaveBeenCalledOnce();
  });

  it('does not retire the replacement when the previous channel closes late', async () => {
    const logger = new Logger<ILogObj>({ minLevel: 7 });
    const initial = channel();
    vi.mocked(getSharedChannel).mockResolvedValueOnce(initial as unknown as Channel);
    const sender = new TestSender(config, logger, breaker());
    await vi.waitFor(() => expect(initial.listenerCount('close')).toBe(1));
    const staleClose = initial.listeners('close')[0] as () => void;

    initial.emit('close');
    await vi.waitFor(() => expect(initial.close).toHaveBeenCalledOnce());
    const replacement = channel();
    vi.mocked(getSharedChannel).mockResolvedValueOnce(replacement as unknown as Channel);
    await sender.reconnect();

    staleClose();
    await Promise.resolve();
    expect(replacement.close).not.toHaveBeenCalled();
  });
});
