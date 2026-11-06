// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import type { IModule, SystemConfig } from '@citrineos/base';
import { RetryMessageError } from '@citrineos/base';
import type { Channel } from 'amqplib';
import { Logger } from 'tslog';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { getSharedChannel } from './shared-connection-pool.js';
import { RabbitMqStationReceiver } from './station-receiver.js';

vi.mock('./shared-connection-pool.js', () => ({ getSharedChannel: vi.fn() }));

function channel() {
  return Object.assign(new EventEmitter(), {
    assertExchange: vi.fn().mockResolvedValue({}),
    assertQueue: vi.fn().mockResolvedValue({}),
    bindQueue: vi.fn().mockResolvedValue({}),
    consume: vi.fn().mockResolvedValue({ consumerTag: 'tag' }),
    close: vi.fn().mockResolvedValue(undefined),
    ack: vi.fn(),
    nack: vi.fn(),
  });
}
const asChannel = (broker: ReturnType<typeof channel>) => broker as unknown as Channel;
const receiver = () =>
  new RabbitMqStationReceiver(
    { util: { messageBroker: { amqp: { url: 'amqp://test', exchange: 'ocpp' } } } } as SystemConfig,
    new Logger({ minLevel: 7 }),
  );

describe('router station channels', () => {
  beforeEach(() => vi.mocked(getSharedChannel).mockReset());

  it('shares concurrent registrations for one station and recovers only the failed channel', async () => {
    const [control, a, b, next] = Array.from({ length: 4 }, channel);
    for (const ch of [control, a, b])
      vi.mocked(getSharedChannel).mockResolvedValueOnce(asChannel(ch));
    const router = receiver();
    await router.initConnection();
    const subscriptions = (id: string) => [
      router.subscribe(id, undefined, { state: 'request' }),
      router.subscribe(id, undefined, { state: 'response' }),
    ];
    await Promise.all([...subscriptions('A'), ...subscriptions('B'), ...subscriptions('A')]);
    expect(a.consume).toHaveBeenCalledTimes(2);
    expect(b.consume).toHaveBeenCalledTimes(2);
    let resume!: (ch: Channel) => void;
    vi.mocked(getSharedChannel).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resume = resolve;
        }),
    );
    a.emit('close');
    await vi.waitFor(() => expect(getSharedChannel).toHaveBeenCalledTimes(4));
    expect(router.disconnectedSince).toBeInstanceOf(Date);
    resume(asChannel(next));
    await vi.waitFor(() => expect(router.disconnectedSince).toBeNull());
    expect(next.consume).toHaveBeenCalledTimes(2);
    expect(b.close).not.toHaveBeenCalled();
    expect(b.consume).toHaveBeenCalledTimes(2);
    await router.unsubscribe('A');
    expect(next.close).toHaveBeenCalledOnce();
    expect(b.close).not.toHaveBeenCalled();
    await router.shutdown();
    expect(b.close).toHaveBeenCalledOnce();
    expect(control.close).toHaveBeenCalledOnce();
  });

  it.each(['success', 'retry'])(
    'does not settle a delivery on a retired channel after handler %s',
    async (outcome) => {
      const broker = channel();
      vi.mocked(getSharedChannel).mockResolvedValue(asChannel(broker));
      const router = receiver();
      let finish!: () => void;
      const pending = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const handle = vi.fn(async () => {
        if (outcome === 'retry') throw new RetryMessageError('Call in progress');
      });
      router.module = { handle } as unknown as IModule;
      await router.subscribe('A');
      const consume = broker.consume.mock.calls[0][1];
      const message = { content: Buffer.from('{}'), properties: {} };
      await consume(message);
      const settle = outcome === 'retry' ? broker.nack : broker.ack;
      expect(settle).toHaveBeenCalledOnce();

      handle.mockImplementationOnce(async () => {
        await pending;
        if (outcome === 'retry') throw new RetryMessageError('Call in progress');
      });
      const delivery = consume(message);
      expect(handle).toHaveBeenCalledTimes(2);

      await router.unsubscribe('A');
      expect(broker.close).toHaveBeenCalledOnce();
      finish();
      await expect(delivery).resolves.toBeUndefined();
      expect(broker.ack).toHaveBeenCalledTimes(outcome === 'success' ? 1 : 0);
      expect(broker.nack).toHaveBeenCalledTimes(outcome === 'retry' ? 1 : 0);
      await router.shutdown();
    },
  );

  it('rejects initial registrations during a broker outage and lets the station try again', async () => {
    vi.useFakeTimers();
    const router = receiver();
    try {
      vi.mocked(getSharedChannel).mockRejectedValue(new Error('Broker offline'));
      const registrations = Promise.allSettled([
        router.subscribe('A', undefined, { state: 'request' }),
        router.subscribe('A', undefined, { state: 'response' }),
        router.subscribe('B'),
      ]);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(getSharedChannel).toHaveBeenCalledTimes(2);
      expect(await registrations).toEqual(
        Array(3).fill({ status: 'rejected', reason: new Error('Broker offline') }),
      );
      expect(vi.getTimerCount()).toBe(0);

      vi.mocked(getSharedChannel).mockResolvedValue(asChannel(channel()));
      await expect(router.subscribe('A')).resolves.toBe(true);
    } finally {
      await router.shutdown();
      vi.useRealTimers();
    }
  });

  it('retires a late channel without disturbing a replacement for the same identifier', async () => {
    const old = channel(),
      replacement = channel();
    let resume!: (ch: Channel) => void;
    vi.mocked(getSharedChannel)
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resume = resolve;
          }),
      )
      .mockResolvedValueOnce(asChannel(replacement));
    const router = receiver();
    const pending = router.subscribe('A');
    await vi.waitFor(() => expect(getSharedChannel).toHaveBeenCalledOnce());
    await router.unsubscribe('A');
    await expect(router.subscribe('A')).resolves.toBe(true);
    const retired = expect(pending).rejects.toThrow('aborted');
    resume(asChannel(old));
    await retired;
    expect(old.close).toHaveBeenCalledOnce();
    expect(old.consume).not.toHaveBeenCalled();
    expect(replacement.close).not.toHaveBeenCalled();
    await router.shutdown();
    expect(replacement.close).toHaveBeenCalledOnce();
    await expect(router.subscribe('A')).resolves.toBe(false);
  });
});
