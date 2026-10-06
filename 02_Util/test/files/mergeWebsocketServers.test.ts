// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { WebsocketServerConfig } from '@citrineos/base';
import { describe, expect, it } from 'vitest';
import { mergeWebsocketServers } from '../../../00_Base/src/config/mergeWebsocketServers.js';

function server(id: string, port: number, host = 'localhost'): WebsocketServerConfig {
  return {
    id,
    host,
    port,
    pingInterval: 60,
    protocol: 'ocpp2.0.1',
    securityProfile: 0,
    allowUnknownChargingStations: false,
    tenantId: 1,
  };
}

describe('mergeWebsocketServers', () => {
  it('keeps a stored server the request does not mention', () => {
    const merged = mergeWebsocketServers(
      [server('1', 9000), server('2', 9002)],
      [server('1', 9000)],
    );

    expect(merged.map((entry) => entry.id)).toEqual(['1', '2']);
  });

  it('uses the request fields when the id exists in storage', () => {
    const merged = mergeWebsocketServers([server('1', 9000)], [server('1', 9000, '10.0.0.8')]);

    expect(merged).toEqual([server('1', 9000, '10.0.0.8')]);
  });

  it('drops a server that exists only in the request', () => {
    const merged = mergeWebsocketServers(
      [server('1', 9000)],
      [server('1', 9000), server('3', 9003)],
    );

    expect(merged.map((entry) => entry.id)).toEqual(['1']);
  });
});
