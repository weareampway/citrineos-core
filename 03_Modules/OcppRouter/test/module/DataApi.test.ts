// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type {
  BootstrapConfig,
  ConfigStore,
  IMessageRouter,
  INetworkConnection,
  SystemConfig,
  WebsocketServerConfig,
} from '@citrineos/base';
import { ConfigStoreFactory } from '@citrineos/base';
import type { IServerNetworkProfileRepository, ISubscriptionRepository } from '@citrineos/data';
import type { FastifyInstance } from 'fastify';
import { Logger } from 'tslog';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AdminApi } from '../../src/module/DataApi.js';

describe('AdminApi websocket lifecycle', () => {
  let api: AdminApi;
  let stored: SystemConfig;
  let listeners: Map<string, WebsocketServerConfig>;
  const upsertProfile = vi.fn();
  const addServer = vi.fn();
  const removeServer = vi.fn();
  const updateConfig = vi.fn();
  const initialServer: WebsocketServerConfig = {
    id: '1',
    host: '127.0.0.1',
    port: 10000,
    pingInterval: 60,
    protocol: 'ocpp2.0.1',
    securityProfile: 0,
    allowUnknownChargingStations: false,
    tenantId: 1,
  };

  beforeEach(() => {
    stored = {
      maxCallLengthSeconds: 5,
      util: { networkConnection: { websocketServers: [structuredClone(initialServer)] } },
    } as SystemConfig;
    listeners = new Map([['1', structuredClone(initialServer)]]);
    upsertProfile.mockReset().mockResolvedValue(undefined);
    addServer.mockReset().mockImplementation(async (config: WebsocketServerConfig) => {
      listeners.set(config.id, config);
    });
    removeServer.mockReset().mockImplementation(async (id: string) => {
      listeners.delete(id);
    });
    updateConfig.mockReset().mockImplementation(async (mutate: (config: SystemConfig) => void) => {
      const draft = structuredClone(stored);
      mutate(draft);
      stored = structuredClone(draft);
      return draft;
    });
    vi.spyOn(ConfigStoreFactory, 'getInstance').mockReturnValue({
      updateConfig,
      fetchConfig: vi.fn(async () => structuredClone(stored)),
    } as unknown as ConfigStore);

    const config = structuredClone(stored) as BootstrapConfig & SystemConfig;
    api = new AdminApi(
      { config } as IMessageRouter,
      {
        addWebsocketServer: addServer,
        removeWebsocketServer: removeServer,
      } as unknown as INetworkConnection,
      { route: vi.fn() } as unknown as FastifyInstance,
      config,
      new Logger({ minLevel: 7 }),
      {} as ISubscriptionRepository,
      { upsertServerNetworkProfile: upsertProfile } as unknown as IServerNetworkProfileRepository,
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function createTenant() {
    return api.addWebsocketConfigurationsForTenant({
      body: { tenantId: 2 },
    } as Parameters<AdminApi['addWebsocketConfigurationsForTenant']>[0]);
  }

  function deleteServer(id: string) {
    return api.deleteWebsocketConfiguration({
      query: { id },
    } as Parameters<AdminApi['deleteWebsocketConfiguration']>[0]);
  }

  function postServer(config: WebsocketServerConfig) {
    return api.createWebsocketConfiguration({
      body: config,
    } as Parameters<AdminApi['createWebsocketConfiguration']>[0]);
  }

  it('finishes activation before a concurrent delete and leaves no deleted listener running', async () => {
    const profileStarted = deferred();
    const profile = deferred();
    upsertProfile.mockImplementationOnce(() => {
      profileStarted.resolve();
      return profile.promise;
    });
    const creation = createTenant();
    await profileStarted.promise;
    let deleted = false;
    const deletion = deleteServer('2').then(() => {
      deleted = true;
    });
    await nextTurn();
    const deletedBeforeActivation = deleted;

    profile.resolve();
    await Promise.all([creation, deletion]);

    expect(deletedBeforeActivation).toBe(false);
    expect(stored.util.networkConnection.websocketServers.map((server) => server.id)).toEqual([
      '1',
      '3',
    ]);
    expect([...listeners.keys()]).toEqual(['1', '3']);
  });

  it('waits for deletion to finish before POST reuses the port', async () => {
    const removalStarted = deferred();
    const removal = deferred();
    removeServer.mockImplementationOnce(async (id: string) => {
      removalStarted.resolve();
      await removal.promise;
      listeners.delete(id);
    });
    const deletion = deleteServer('1');
    await removalStarted.promise;
    let posted = false;
    const replacement = { ...initialServer, id: '2', tenantId: 2 };
    const creation = postServer(replacement).then(() => {
      posted = true;
    });
    await nextTurn();
    const postedBeforeRemoval = posted;

    removal.resolve();
    await Promise.all([deletion, creation]);

    expect(postedBeforeRemoval).toBe(false);
    expect(stored.util.networkConnection.websocketServers).toEqual([replacement]);
    expect(listeners.has('1')).toBe(false);
  });

  it('lets a queued deletion proceed after a profile upsert fails', async () => {
    const profileStarted = deferred();
    const profile = deferred();
    upsertProfile.mockImplementationOnce(() => {
      profileStarted.resolve();
      return profile.promise;
    });
    const creation = expect(createTenant()).rejects.toThrow('profile unavailable');
    await profileStarted.promise;
    const deletion = deleteServer('2');

    profile.reject(new Error('profile unavailable'));
    await Promise.all([creation, deletion]);

    expect(stored.util.networkConnection.websocketServers.some((server) => server.id === '2')).toBe(
      false,
    );
    expect(listeners.has('2')).toBe(false);
    expect(addServer).not.toHaveBeenCalled();
    expect(removeServer).toHaveBeenCalledWith('2');
  });

  it('retries a failed listener start without duplicating the persisted configuration', async () => {
    addServer.mockRejectedValueOnce(new Error('listen failed'));
    await expect(createTenant()).rejects.toThrow('listen failed');

    const servers = await createTenant();

    expect(servers.map((server) => server.id)).toEqual(['2', '3']);
    expect(stored.util.networkConnection.websocketServers.map((server) => server.id)).toEqual([
      '1',
      '2',
      '3',
    ]);
    expect([...listeners.keys()]).toEqual(['1', '2', '3']);
  });

  it('does not activate listeners after a failed config write and accepts the next request', async () => {
    updateConfig.mockRejectedValueOnce(new Error('storage unavailable'));
    await expect(createTenant()).rejects.toThrow('storage unavailable');
    expect(upsertProfile).not.toHaveBeenCalled();
    expect(addServer).not.toHaveBeenCalled();

    const replacement = { ...initialServer, id: '2', port: 10001, tenantId: 2 };
    await expect(postServer(replacement)).resolves.toEqual(replacement);
  });
});

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
