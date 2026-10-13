// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { Channel } from 'amqplib';
import {
  RABBITMQ_DISCONNECT_UNHEALTHY_MS,
  RabbitMqConnectionHealth,
  rabbitMqClientsAreUnhealthy,
  retireRabbitChannel,
} from './connection-health.js';

describe('RabbitMqConnectionHealth', () => {
  it('treats further errors during a reconnect as the same outage', () => {
    const health = new RabbitMqConnectionHealth();
    const first = new Date('2026-10-13T02:00:00Z');

    expect(health.beginDisconnect(first)).toBe(true);
    expect(health.beginDisconnect(new Date('2026-10-13T02:00:01Z'))).toBe(false);
    expect(health.disconnectedSince).toEqual(first);
  });

  it('accepts a new disconnect after the connection is back', () => {
    const health = new RabbitMqConnectionHealth();
    health.beginDisconnect(new Date('2026-10-13T02:00:00Z'));
    health.markConnected();

    const again = new Date('2026-10-13T03:00:00Z');
    expect(health.beginDisconnect(again)).toBe(true);
    expect(health.disconnectedSince).toEqual(again);
  });
});

describe('rabbitMqClientsAreUnhealthy', () => {
  const now = new Date('2026-10-13T02:02:01Z');

  it('stays healthy until a client has been disconnected for more than 120 seconds', () => {
    const justInside = new Date(now.getTime() - RABBITMQ_DISCONNECT_UNHEALTHY_MS);
    expect(rabbitMqClientsAreUnhealthy([{ disconnectedSince: justInside }], now)).toBe(false);
    expect(rabbitMqClientsAreUnhealthy([{ disconnectedSince: null }], now)).toBe(false);
  });

  it('is unhealthy when any client has been disconnected for longer than 120 seconds', () => {
    const tooLong = new Date(now.getTime() - RABBITMQ_DISCONNECT_UNHEALTHY_MS - 1);
    expect(
      rabbitMqClientsAreUnhealthy(
        [{ disconnectedSince: null }, { disconnectedSince: tooLong }],
        now,
      ),
    ).toBe(true);
  });
});

describe('retireRabbitChannel', () => {
  it('removes listeners and closes the channel', async () => {
    const channel = {
      removeAllListeners: vi.fn(),
      close: vi.fn().mockResolvedValue(undefined),
    } as unknown as Channel;

    await retireRabbitChannel(channel);

    expect(channel.removeAllListeners).toHaveBeenCalledWith('error');
    expect(channel.removeAllListeners).toHaveBeenCalledWith('close');
    expect(channel.close).toHaveBeenCalled();
  });

  it('ignores a channel the broker already closed', async () => {
    const channel = {
      removeAllListeners: vi.fn(),
      close: vi.fn().mockRejectedValue(new Error('Channel closed')),
    } as unknown as Channel;

    await expect(retireRabbitChannel(channel)).resolves.toBeUndefined();
  });
});
