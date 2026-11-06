// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type {
  BootstrapConfig,
  IAuthenticator,
  IMessageSender,
  SystemConfig,
} from '@citrineos/base';
import { CacheNamespace, MessageOrigin, MessageState } from '@citrineos/base';
import type { ILocationRepository } from '@citrineos/data';
import { MemoryCache, RabbitMqStationReceiver, WebsocketNetworkConnection } from '@citrineos/util';
import { clearPoolForUrl } from '@citrineos/util/dist/queue/rabbit-mq/shared-connection-pool.js';
import * as amqp from 'amqplib';
import { type ILogObj, Logger } from 'tslog';
import { expect, it, vi } from 'vitest';
import { WebSocket } from 'ws';
import { MessageRouterImpl } from '../../src/module/router.js';
import type { WebhookDispatcher } from '../../src/module/webhook.dispatcher.js';

const url = process.env.CITRINEOS_TEST_RABBITMQ_URL;
it.skipIf(!url)(
  'delivers one published Reset to the replacement WebSocket after a mid-send disconnect',
  async () => {
    if (!url || !['localhost', '127.0.0.1', '[::1]'].includes(new URL(url).hostname)) {
      throw new Error('Use a disposable local RabbitMQ broker for this test.');
    }
    const prefix = `reconnect-${randomUUID()}-`;
    const config = {
      maxCallLengthSeconds: 1,
      util: {
        networkConnection: { websocketServers: [] },
        messageBroker: { amqp: { url, exchange: prefix, queuePrefix: prefix } },
      },
    } as unknown as BootstrapConfig & SystemConfig;
    const logger = new Logger<ILogObj>({ minLevel: 7 });
    const cache = new MemoryCache();
    const receiver = new RabbitMqStationReceiver(config, logger);
    const sender = { send: vi.fn().mockResolvedValue({ success: true }) };
    const dispatcher = {
      register: vi.fn(),
      deregister: vi.fn().mockResolvedValue(undefined),
      dispatchMessageSent: vi.fn().mockResolvedValue(undefined),
      dispatchMessageReceivedUnparsed: vi.fn().mockResolvedValue(undefined),
      dispatchMessageReceived: vi.fn().mockResolvedValue(undefined),
    };
    const location = {
      setChargingStationIsOnlineAndOCPPVersion: vi.fn().mockResolvedValue(undefined),
      readChargingStationByStationId: vi.fn().mockResolvedValue(undefined),
    };
    const router = new MessageRouterImpl(
      config,
      cache,
      sender as unknown as IMessageSender,
      receiver,
      dispatcher as unknown as WebhookDispatcher,
      async () => {},
      logger,
      undefined,
      location as unknown as ILocationRepository,
    );
    const network = new WebsocketNetworkConnection(
      config,
      cache,
      { authenticate: async () => ({ identifier: 'A' }) } as unknown as IAuthenticator,
      router,
      logger,
    );
    router.networkHook = network.bindNetworkHook();
    const probe = await amqp.connect(url);
    const sockets: WebSocket[] = [];
    let releaseLookup!: () => void;
    const pendingLookup = new Promise<void>((resolve) => {
      releaseLookup = resolve;
    });
    const channel = await probe.createChannel();
    try {
      await network.addWebsocketServer({
        id: 'test',
        tenantId: 1,
        host: '127.0.0.1',
        port: 0,
        protocol: 'ocpp1.6',
        securityProfile: 0,
        allowUnknownChargingStations: true,
        pingInterval: 60,
      });
      const internals = network as unknown as {
        _httpServersMap: Map<string, Server>;
        _ping: () => Promise<void>;
      };
      vi.spyOn(internals, '_ping').mockResolvedValue(undefined);
      const port = (internals._httpServersMap.get('test')!.address() as AddressInfo).port;
      const connect = () => {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/A`, 'ocpp1.6');
        sockets.push(ws);
        return ws;
      };
      await channel.assertExchange(prefix, 'headers', { durable: false });
      const registration = vi.spyOn(router, 'registerConnection');
      const first = connect();
      await once(first, 'open');
      await vi.waitFor(() => expect(registration).toHaveBeenCalledOnce());
      await expect(registration.mock.results[0].value).resolves.toBe(true);

      // Pause ONLY the first send after it has acquired the transaction lock.
      const get = cache.get.bind(cache);
      let lookupStarted = false;
      vi.spyOn(cache, 'get').mockImplementation(async (...args) => {
        if (!lookupStarted && args[1] === CacheNamespace.Connections) {
          lookupStarted = true;
          await pendingLookup;
        }
        return get(...args);
      });
      const handle = router.handle.bind(router);
      let completed = 0;
      vi.spyOn(router, 'handle').mockImplementation(async (message) => {
        await handle(message);
        completed++;
      });
      const deregister = vi.spyOn(router, 'deregisterConnection');
      const headers = {
        stationId: 'A',
        tenantId: '1',
        origin: MessageOrigin.ChargingStationManagementSystem,
        state: MessageState.Request.toString(),
      };
      const reset = [2, 'reset-1', 'Reset', { type: 'Soft' }] as const;
      channel.publish(
        prefix,
        '',
        Buffer.from(
          JSON.stringify({
            ...headers,
            state: MessageState.Request,
            action: reset[2],
            context: { stationId: 'A', tenantId: 1, correlationId: reset[1] },
            payload: reset[3],
            protocol: 'ocpp1.6',
          }),
        ),
        { headers },
      );
      await vi.waitFor(() => expect(lookupStarted).toBe(true));
      first.close();
      await vi.waitFor(() => expect(deregister).toHaveBeenCalledOnce());
      await deregister.mock.results[0].value;

      const replacement = connect();
      const frames: unknown[][] = [];
      let barrierSeen = false;
      replacement.on('message', (data) => {
        const frame = JSON.parse(data.toString());
        if (frame[1] === 'barrier') {
          barrierSeen = true;
          return;
        }
        frames.push(frame);
        replacement.send(JSON.stringify([3, frame[1], { status: 'Accepted' }]));
      });
      await once(replacement, 'open');
      await vi.waitFor(() => expect(registration).toHaveBeenCalledTimes(2));
      await expect(registration.mock.results[1].value).resolves.toBe(true);
      releaseLookup();

      // Wait for both the retired handler and the broker's redelivery. A final frame
      // on the same socket flushes every preceding Reset without a timing-based sleep.
      await vi.waitFor(() => expect(completed).toBe(2), { timeout: 5000 });
      await network.sendMessage('1:A', JSON.stringify([2, 'barrier', 'GetConfiguration', {}]));
      await vi.waitFor(() => expect(barrierSeen).toBe(true));
      expect(frames).toEqual([reset]);
      await vi.waitFor(() => expect(sender.send).toHaveBeenCalledOnce());
    } finally {
      releaseLookup();
      for (const socket of sockets) socket.terminate();
      await network.removeWebsocketServer('test');
      await receiver.shutdown();
      clearPoolForUrl(url);
      await channel.deleteExchange(prefix);
      await probe.close();
      vi.restoreAllMocks();
    }
  },
  15000,
);
