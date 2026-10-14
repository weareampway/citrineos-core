// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { type ILogObj, Logger } from 'tslog';
import {
  Message,
  MessageOrigin,
  MessageState,
  EventGroup,
  OCPP1_6_CallAction,
  OCPPVersion,
} from '@citrineos/base';
import type { SystemConfig } from '@citrineos/base';
import { RabbitMqSender } from './sender.js';
import { RabbitMqReceiver } from './receiver.js';
import { clearPoolForUrl, getSharedChannel } from './shared-connection-pool.js';

const url = process.env.RABBITMQ_TEST_URL;

class Sender extends RabbitMqSender {
  ready() {
    return this._connectWithRetry();
  }
}
class Receiver extends RabbitMqReceiver {
  ready() {
    return this._connectWithRetry();
  }
}

// Opt-in only: use a disposable local broker, never an application configuration.
describe.skipIf(!url)('RabbitMQ real transport recovery', () => {
  it('restores all consumers and delivers messages after repeated transport loss', async () => {
    if (!url || !['127.0.0.1', 'localhost'].includes(new URL(url).hostname)) {
      throw new Error('RABBITMQ_TEST_URL must point to a disposable loopback broker');
    }
    const config = {
      maxReconnectDelay: 30,
      util: {
        messageBroker: { amqp: { url, exchange: 'synthetic-recovery', queuePrefix: 'synthetic_' } },
      },
    } as SystemConfig;
    const logger = new Logger<ILogObj>({ minLevel: 7 });
    const receivers = [new Receiver(config, logger), new Receiver(config, logger)];
    const sender = new Sender(config, logger);
    const handlers = receivers.map((receiver) =>
      vi.spyOn(receiver, 'handle').mockResolvedValue(undefined),
    );
    try {
      await Promise.all([
        sender.ready(),
        ...receivers.map((receiver) => receiver.initConnection()),
      ]);
      for (let index = 0; index < receivers.length; index++)
        await receivers[index].subscribe(`consumer-${index}`);
      for (let round = 0; round < 3; round++) {
        if (round > 0) {
          const probe = await getSharedChannel(url);
          const closed = new Promise<void>((resolve) => probe.once('close', resolve));
          clearPoolForUrl(url);
          await closed;
          await Promise.all([sender.ready(), ...receivers.map((receiver) => receiver.ready())]);
        }
        const message = new Message(
          MessageOrigin.ChargingStation,
          EventGroup.Monitoring,
          OCPP1_6_CallAction.Heartbeat,
          MessageState.Request,
          {
            tenantId: 1,
            stationId: 'synthetic-station',
            correlationId: `round-${round}`,
            timestamp: '2026-01-01T00:00:00Z',
          },
          {},
          OCPPVersion.OCPP1_6,
        );
        expect((await sender.send(message)).success).toBe(true);
        await vi.waitFor(() =>
          handlers.forEach((handler) => expect(handler).toHaveBeenCalledTimes(round + 1)),
        );
      }
      expect(sender.disconnectedSince).toBeNull();
      receivers.forEach((receiver) => expect(receiver.disconnectedSince).toBeNull());
    } finally {
      await Promise.all([sender.shutdown(), ...receivers.map((receiver) => receiver.shutdown())]);
      clearPoolForUrl(url);
    }
  }, 20_000);
});
