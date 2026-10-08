// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { SystemConfig, WebsocketServerConfig } from '@citrineos/base';
import { describe, expect, it, vi } from 'vitest';
import {
  commitConfigUpdate,
  ConfigVersionConflictError,
} from '../../src/files/commitConfigUpdate.js';

function configWith(ports: number[]): SystemConfig {
  return {
    util: {
      networkConnection: {
        websocketServers: ports.map(
          (port) => ({ id: String(port), port }) as WebsocketServerConfig,
        ),
      },
    },
  } as SystemConfig;
}

describe('commitConfigUpdate', () => {
  it('applies a stale change onto the newer config after a version conflict', async () => {
    let stored = configWith([9000]);
    let version = 'v1';

    const load = vi.fn(async () => ({
      config: structuredClone(stored),
      version,
    }));
    const save = vi.fn(async (config: SystemConfig, savedVersion: string) => {
      if (savedVersion === 'v1') {
        stored = configWith([9000, 9002]);
        version = 'v2';
        throw new ConfigVersionConflictError();
      }
      stored = structuredClone(config);
      version = 'v3';
    });

    const result = await commitConfigUpdate(load, save, (config) => {
      config.util.networkConnection.websocketServers.push({
        id: '9003',
        port: 9003,
      } as WebsocketServerConfig);
    });

    expect(result.util.networkConnection.websocketServers.map((server) => server.port)).toEqual([
      9000, 9002, 9003,
    ]);
    expect(load).toHaveBeenCalledTimes(2);
    expect(save).toHaveBeenCalledTimes(2);
  });

  it('does not retry a change that fails for another reason', async () => {
    const load = vi.fn(async () => ({ config: configWith([9000]), version: 'v1' }));
    const save = vi.fn();

    await expect(
      commitConfigUpdate(load, save, () => {
        throw new Error('duplicate port');
      }),
    ).rejects.toThrow('duplicate port');

    expect(load).toHaveBeenCalledTimes(1);
    expect(save).not.toHaveBeenCalled();
  });

  it('throws when no config is stored', async () => {
    await expect(
      commitConfigUpdate(
        async () => null,
        vi.fn(),
        () => {},
      ),
    ).rejects.toThrow('No configuration found in storage');
  });
});
