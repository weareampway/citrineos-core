// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { CallAction, SystemConfig } from '@citrineos/base';
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
    unbindQueue: vi.fn().mockResolvedValue({}),
    deleteQueue: vi.fn().mockResolvedValue({ messageCount: 0 }),
    consume: vi.fn().mockResolvedValue({ consumerTag: 'ctag' }),
    cancel: vi.fn().mockResolvedValue({}),
    close: vi.fn().mockResolvedValue(undefined),
  });
}

/** Holds back the next consume reply; call the result once consume has been called. */
function holdNextConsume(broker: ReturnType<typeof channel>, consumerTag: string) {
  let release!: () => void;
  broker.consume.mockImplementationOnce(
    () =>
      new Promise<{ consumerTag: string }>((resolve) => {
        release = () => resolve({ consumerTag });
      }),
  );
  return () => release();
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
    const receiver = new TestReceiver(config, new Logger({ minLevel: 7 }));
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
    const finishConsume = holdNextConsume(restored, 'restored');
    vi.mocked(getSharedChannel).mockResolvedValue(restored as unknown as Channel);
    await vi.advanceTimersByTimeAsync(1000);
    const attempt = receiver.reconnect();
    expect(receiver.reconnect()).toBe(attempt);
    await vi.waitFor(() => expect(restored.consume).toHaveBeenCalledOnce());
    expect(rabbitMqClientsAreUnhealthy([receiver], new Date())).toBe(true);

    finishConsume();
    await attempt;
    expect(restored.consume).toHaveBeenCalledOnce();
    expect(receiver.disconnectedSince).toBeNull();
    expect(rabbitMqClientsAreUnhealthy([receiver], new Date())).toBe(false);
  });
});

describe('RabbitMqReceiver subscriptions', () => {
  const config = {
    maxReconnectDelay: 30,
    util: { messageBroker: { amqp: { url: 'amqp://test', exchange: 'ocpp' } } },
  } as SystemConfig;

  async function connectedReceiver(broker: ReturnType<typeof channel>) {
    vi.mocked(getSharedChannel).mockResolvedValueOnce(broker as unknown as Channel);
    const receiver = new TestReceiver(config, new Logger({ minLevel: 7 }));
    await receiver.initConnection();
    return receiver;
  }

  beforeEach(() => {
    vi.mocked(getSharedChannel).mockReset();
  });

  it('cancels both consumers of one station and does not delete the shared queue', async () => {
    const broker = channel();
    broker.consume
      .mockResolvedValueOnce({ consumerTag: 'request' })
      .mockResolvedValueOnce({ consumerTag: 'response' });
    const receiver = await connectedReceiver(broker);
    await receiver.subscribe('9:Wallbox6', undefined, { state: '1' });
    await receiver.subscribe('9:Wallbox6', undefined, { state: '2' });

    await expect(receiver.unsubscribe('9:Wallbox6')).resolves.toBe(true);

    expect(broker.cancel).toHaveBeenCalledTimes(2);
    expect(broker.cancel).toHaveBeenNthCalledWith(1, 'request');
    expect(broker.cancel).toHaveBeenNthCalledWith(2, 'response');
    expect(broker.deleteQueue).not.toHaveBeenCalled();
    expect(broker.unbindQueue).not.toHaveBeenCalled();
    await receiver.shutdown();
  });

  it('declares station queues to outlive their last consumer, module queues unchanged', async () => {
    // Cancelling the last consumer must not delete a station queue that a concurrent
    // subscribe (on another instance or this channel) has already declared.
    const broker = channel();
    const receiver = await connectedReceiver(broker);
    await receiver.subscribe('9:Wallbox6');
    await receiver.subscribe('Transactions_requests', [
      'StartTransaction',
    ] as unknown as CallAction[]);

    expect(broker.assertQueue).toHaveBeenNthCalledWith(1, 'rabbit_queue_exp_9:Wallbox6', {
      durable: false,
      exclusive: false,
      expires: 30_000,
    });
    expect(broker.assertQueue).toHaveBeenNthCalledWith(2, 'rabbit_queue_Transactions_requests', {
      durable: false,
      autoDelete: true,
      exclusive: false,
    });
    await receiver.shutdown();
  });

  it('cancels a consumer whose subscribe was still in flight when unsubscribed', async () => {
    const broker = channel();
    const release = holdNextConsume(broker, 'late');
    const receiver = await connectedReceiver(broker);

    const pending = receiver.subscribe('9:Wallbox6');
    await vi.waitFor(() => expect(broker.consume).toHaveBeenCalledOnce());
    await expect(receiver.unsubscribe('9:Wallbox6')).resolves.toBe(true);
    release();

    await expect(pending).resolves.toBe(false);
    expect(broker.cancel).toHaveBeenCalledWith('late');

    const restored = channel();
    vi.mocked(getSharedChannel).mockResolvedValue(restored as unknown as Channel);
    broker.emit('close');
    await receiver.reconnect();
    expect(restored.consume).not.toHaveBeenCalled();
    await receiver.shutdown();
  });

  it('does not restore subscriptions that are unsubscribed during a reconnect', async () => {
    const initial = channel();
    const receiver = await connectedReceiver(initial);
    await receiver.subscribe('9:A');
    await receiver.subscribe('9:B');

    const restored = channel();
    const release = holdNextConsume(restored, 'a');
    vi.mocked(getSharedChannel).mockResolvedValue(restored as unknown as Channel);
    initial.emit('close');
    const attempt = receiver.reconnect();
    await vi.waitFor(() => expect(restored.consume).toHaveBeenCalledOnce());

    await receiver.unsubscribe('9:A');
    await receiver.unsubscribe('9:B');
    release();
    await attempt;

    // 9:B is skipped; 9:A's consumer is cancelled once its consume has returned.
    expect(restored.consume).toHaveBeenCalledOnce();
    expect(restored.cancel).toHaveBeenCalledOnce();
    expect(restored.cancel).toHaveBeenCalledWith('a');
    await receiver.shutdown();
  });

  it('does not cancel a previous channel consumer tag after reconnect', async () => {
    const initial = channel();
    initial.consume.mockResolvedValueOnce({ consumerTag: 'old' });
    const receiver = await connectedReceiver(initial);
    await receiver.subscribe('9:Wallbox6');

    const restored = channel();
    restored.consume.mockResolvedValue({ consumerTag: 'new' });
    vi.mocked(getSharedChannel).mockResolvedValue(restored as unknown as Channel);
    initial.emit('close');
    await receiver.reconnect();

    expect(initial.cancel).not.toHaveBeenCalled();
    expect(restored.cancel).not.toHaveBeenCalled();
    expect(restored.consume).toHaveBeenCalledOnce();
    expect(restored.deleteQueue).not.toHaveBeenCalled();

    await expect(receiver.unsubscribe('9:Wallbox6')).resolves.toBe(true);
    expect(restored.cancel).toHaveBeenCalledOnce();
    expect(restored.cancel).toHaveBeenCalledWith('new');
    expect(initial.cancel).not.toHaveBeenCalled();
    expect(initial.deleteQueue).not.toHaveBeenCalled();
    await receiver.shutdown();
  });

  it('reuses an identical subscription instead of adding a consumer', async () => {
    const broker = channel();
    const receiver = await connectedReceiver(broker);

    await receiver.subscribe('9:Wallbox6', undefined, { state: '1' });
    await expect(receiver.subscribe('9:Wallbox6', undefined, { state: '1' })).resolves.toBe(true);

    expect(broker.consume).toHaveBeenCalledOnce();
    await receiver.unsubscribe('9:Wallbox6');
    expect(broker.cancel).toHaveBeenCalledOnce();
    await receiver.shutdown();
  });

  it('subscribes again when the broker cancels the consumer', async () => {
    const broker = channel();
    broker.consume
      .mockResolvedValueOnce({ consumerTag: 'first' })
      .mockResolvedValueOnce({ consumerTag: 'second' });
    const receiver = await connectedReceiver(broker);
    await receiver.subscribe('9:Wallbox6');

    const onMessage = broker.consume.mock.calls[0][1] as (msg: null) => void;
    onMessage(null);
    await vi.waitFor(() => expect(broker.consume).toHaveBeenCalledTimes(2));

    await receiver.unsubscribe('9:Wallbox6');
    expect(broker.cancel).toHaveBeenCalledOnce();
    expect(broker.cancel).toHaveBeenCalledWith('second');
    await receiver.shutdown();
  });

  it('reports a failed cancel and treats a repeated unsubscribe as done', async () => {
    const broker = channel();
    broker.cancel.mockRejectedValueOnce(new Error('channel closing'));
    const receiver = await connectedReceiver(broker);
    await receiver.subscribe('9:Wallbox6');

    await expect(receiver.unsubscribe('9:Wallbox6')).resolves.toBe(false);
    await expect(receiver.unsubscribe('9:Wallbox6')).resolves.toBe(true);
    await receiver.shutdown();
  });
});
