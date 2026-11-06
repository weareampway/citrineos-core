// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { IModule, SystemConfig } from '@citrineos/base';
import * as amqp from 'amqplib';
import { Logger } from 'tslog';
import { expect, it, vi } from 'vitest';
import { clearPoolForUrl } from './shared-connection-pool.js';
import { RabbitMqStationReceiver } from './station-receiver.js';

// Run against a disposable RabbitMQ 4.2 broker with consumer_max_per_channel=2.
const url = process.env.CITRINEOS_TEST_RABBITMQ_URL;
it.skipIf(!url)(
  'registers 55 stations with two consumers each and releases only the disconnected station',
  async () => {
    if (!url || !['localhost', '127.0.0.1', '[::1]'].includes(new URL(url).hostname)) {
      throw new Error('Use a disposable local RabbitMQ broker for this test.');
    }
    const prefix = `test-${randomUUID()}-`;
    const probe = await amqp.connect(url);
    const router = new RabbitMqStationReceiver(
      {
        util: { messageBroker: { amqp: { url, exchange: prefix, queuePrefix: prefix } } },
      } as SystemConfig,
      new Logger({ minLevel: 7 }),
    );
    const handle = vi.fn().mockResolvedValue(undefined);
    router.module = { handle } as unknown as IModule;
    const stations = Array.from({ length: 55 }, (_, i) => `1:station-${i}`);
    try {
      const channel = await probe.createChannel();
      await router.initConnection();
      const subscribe = (id: string) =>
        Promise.all([
          router.subscribe(id, undefined, { stationId: id, state: 'request' }),
          router.subscribe(id, undefined, { stationId: id, state: 'response' }),
        ]);
      const registrations = await Promise.all(stations.map(subscribe));
      expect(registrations.flat().every(Boolean)).toBe(true);
      for (const id of stations) {
        expect((await channel.checkQueue(`${prefix}exp_${id}`)).consumerCount).toBe(2);
      }
      await router.unsubscribe(stations[0]);
      expect((await channel.checkQueue(`${prefix}exp_${stations[0]}`)).consumerCount).toBe(0);
      expect((await channel.checkQueue(`${prefix}exp_${stations[1]}`)).consumerCount).toBe(2);
      const message = { action: 'Heartbeat', context: { stationId: stations[1] }, payload: {} };
      channel.publish(prefix, '', Buffer.from(JSON.stringify(message)), {
        headers: { stationId: stations[1], state: 'request' },
      });
      await vi.waitFor(() => expect(handle).toHaveBeenCalledOnce());
      expect(handle.mock.calls[0][0].context.stationId).toBe(stations[1]);
      expect(await subscribe(stations[0])).toEqual([true, true]);
      expect((await channel.checkQueue(`${prefix}exp_${stations[0]}`)).consumerCount).toBe(2);
      await channel.deleteExchange(prefix);
    } finally {
      await router.shutdown();
      clearPoolForUrl(url);
      await probe.close();
    }
  },
  30000,
);
