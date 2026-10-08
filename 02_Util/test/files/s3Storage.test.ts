// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import { PutObjectCommand } from '@aws-sdk/client-s3';
import type { BootstrapConfig, SystemConfig, WebsocketServerConfig } from '@citrineos/base';
import { Readable } from 'stream';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { S3Storage } from '../../src/files/s3Storage.js';

const send = vi.hoisted(() => vi.fn());

vi.mock('@aws-sdk/client-s3', () => {
  class GetObjectCommand {
    constructor(public input: unknown) {}
  }
  class PutObjectCommand {
    constructor(public input: { IfMatch?: string; Body?: Buffer }) {}
  }
  class CreateBucketCommand {
    constructor(public input: unknown) {}
  }
  class S3Client {
    send = send;
  }
  return { S3Client, GetObjectCommand, PutObjectCommand, CreateBucketCommand };
});

function configWith(ports: number[]): SystemConfig {
  return {
    util: {
      networkConnection: {
        websocketServers: ports.map(
          (port) =>
            ({
              id: String(port),
              host: 'localhost',
              port,
              pingInterval: 60,
              protocol: 'ocpp2.0.1',
              securityProfile: 0,
              allowUnknownChargingStations: false,
              tenantId: 1,
            }) as WebsocketServerConfig,
        ),
      },
    },
  } as SystemConfig;
}

function objectBody(config: SystemConfig): Readable {
  return Readable.from([Buffer.from(JSON.stringify(config))]);
}

describe('S3Storage.updateConfig', () => {
  beforeEach(() => {
    send.mockReset();
  });

  it('retries a version conflict and keeps the intervening server', async () => {
    send
      .mockResolvedValueOnce({
        Body: objectBody(configWith([9000])),
        ETag: '"v1"',
      })
      .mockRejectedValueOnce(
        Object.assign(new Error('precondition'), { $metadata: { httpStatusCode: 412 } }),
      )
      .mockResolvedValueOnce({
        Body: objectBody(configWith([9000, 9002])),
        ETag: '"v2"',
      })
      .mockResolvedValueOnce({ $metadata: { httpStatusCode: 200 } });

    const storage = new S3Storage(
      {
        region: 'eu-central-1',
        defaultBucketName: 'config-bucket',
      } as BootstrapConfig['fileAccess']['s3'],
      'config.json',
    );

    const updated = await storage.updateConfig((config) => {
      config.util.networkConnection.websocketServers.push({
        id: '9003',
        host: 'localhost',
        port: 9003,
        pingInterval: 60,
        protocol: 'ocpp2.0.1',
        securityProfile: 0,
        allowUnknownChargingStations: false,
        tenantId: 1,
      });
    });

    expect(updated.util.networkConnection.websocketServers.map((server) => server.port)).toEqual([
      9000, 9002, 9003,
    ]);
    const puts = send.mock.calls
      .map(([command]) => command)
      .filter((command) => command instanceof PutObjectCommand);
    expect(puts.map((command: PutObjectCommand) => command.input.IfMatch)).toEqual([
      '"v1"',
      '"v2"',
    ]);
  });
});
