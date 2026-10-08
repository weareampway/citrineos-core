// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type {
  IAuthenticator,
  ICache,
  IMessageRouter,
  SystemConfig,
  WebsocketServerConfig,
} from '@citrineos/base';
import { once } from 'node:events';
import type { Server } from 'node:http';
import { connect, type AddressInfo, type Socket } from 'node:net';
import { Logger } from 'tslog';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { WebSocket } from 'ws';
import { WebsocketNetworkConnection } from '../../src/networkconnection/WebsocketNetworkConnection.js';

describe('WebsocketNetworkConnection server deletion', () => {
  let connection: WebsocketNetworkConnection;
  let httpServers: Map<string, Server>;
  let clients: WebSocket[];
  let sockets: Socket[];
  const registerConnection = vi.fn();
  const deregisterConnection = vi.fn();
  const onMessage = vi.fn();
  const authenticate = vi.fn();

  beforeEach(() => {
    clients = [];
    sockets = [];
    registerConnection.mockReset().mockResolvedValue(true);
    deregisterConnection.mockReset().mockResolvedValue(true);
    onMessage.mockReset().mockResolvedValue(true);
    authenticate.mockReset().mockResolvedValue({ identifier: 'charger' });
    connection = new WebsocketNetworkConnection(
      { util: { networkConnection: { websocketServers: [] } } } as SystemConfig,
      {
        set: vi.fn().mockResolvedValue(true),
        remove: vi.fn().mockResolvedValue(true),
      } as unknown as ICache,
      { authenticate } as unknown as IAuthenticator,
      { registerConnection, deregisterConnection, onMessage } as unknown as IMessageRouter,
      new Logger({ minLevel: 7 }),
    );
    const internals = connection as unknown as {
      _httpServersMap: Map<string, Server>;
      _ping: () => Promise<void>;
    };
    httpServers = internals._httpServersMap;
    // Keepalive scheduling is independent of listener shutdown.
    vi.spyOn(internals, '_ping').mockResolvedValue(undefined);
  });

  afterEach(async () => {
    for (const client of clients) client.terminate();
    for (const socket of sockets) socket.destroy();
    await Promise.all([...httpServers.keys()].map((id) => connection.removeWebsocketServer(id)));
    vi.restoreAllMocks();
  });

  async function startServer(id: string, port = 0): Promise<WebsocketServerConfig> {
    const config: WebsocketServerConfig = {
      id,
      host: '127.0.0.1',
      port,
      protocol: 'ocpp1.6',
      pingInterval: 60,
      securityProfile: 0,
      allowUnknownChargingStations: false,
      tenantId: 1,
    };
    await connection.addWebsocketServer(config);
    return { ...config, port: (httpServers.get(id)!.address() as AddressInfo).port };
  }

  async function connectCharger(config: WebsocketServerConfig): Promise<WebSocket> {
    const client = new WebSocket(
      `ws://${config.host}:${config.port}/charger-${config.id}`,
      'ocpp1.6',
    );
    clients.push(client);
    await once(client, 'open');
    return client;
  }

  it('disconnects a connected charger, releases the port, and allows repeated deletion and recreation', async () => {
    const config = await startServer('1');
    const client = await connectCharger(config);
    const closed = once(client, 'close');

    await withinDeadline(Promise.all([connection.removeWebsocketServer('1'), closed]));

    expect(client.readyState).toBe(WebSocket.CLOSED);
    expect(httpServers.has('1')).toBe(false);
    await vi.waitFor(() => expect(deregisterConnection).toHaveBeenCalledWith(1, 'charger-1'));
    await connection.removeWebsocketServer('1');
    await startServer('1', config.port);
    const reconnected = await connectCharger(config);
    expect(reconnected.readyState).toBe(WebSocket.OPEN);
  });

  it('keeps chargers connected to other servers', async () => {
    const first = await connectCharger(await startServer('1'));
    const second = await connectCharger(await startServer('2'));
    const closed = once(first, 'close');

    await withinDeadline(Promise.all([connection.removeWebsocketServer('1'), closed]));

    expect(second.readyState).toBe(WebSocket.OPEN);
    second.send('still connected');
    await vi.waitFor(() =>
      expect(onMessage).toHaveBeenCalledWith(
        expect.any(String),
        'still connected',
        expect.any(Date),
        'ocpp1.6',
      ),
    );
    expect(deregisterConnection).not.toHaveBeenCalledWith(1, 'charger-2');
  });

  it('closes sockets whose websocket authentication is still pending', async () => {
    let authenticationStarted!: () => void;
    const authenticating = new Promise<void>((resolve) => {
      authenticationStarted = resolve;
    });
    authenticate.mockImplementation(() => {
      authenticationStarted();
      return new Promise(() => {});
    });
    const config = await startServer('1');
    const socket = connect(config.port, config.host);
    sockets.push(socket);
    await once(socket, 'connect');
    socket.write(
      'GET /charger HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n',
    );
    await authenticating;
    const closed = once(socket, 'close');

    await withinDeadline(Promise.all([connection.removeWebsocketServer('1'), closed]));

    expect(socket.destroyed).toBe(true);
  });
});

async function withinDeadline<T>(operation: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Server deletion did not complete')), 1000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
