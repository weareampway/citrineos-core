// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import type { SystemConfig } from '@citrineos/base';
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
