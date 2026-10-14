// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { Channel } from 'amqplib';

export const RABBITMQ_DISCONNECT_UNHEALTHY_MS = 120_000;

export interface RabbitMqDisconnectState {
  disconnectedSince: Date | null;
}

/**
 * One disconnect starts a reconnect. Further channel errors while that
 * reconnect is in flight are the same outage, not a new one.
 */
export class RabbitMqConnectionHealth {
  private reconnecting = false;
  disconnectedSince: Date | null = null;

  beginDisconnect(now = new Date()): boolean {
    if (this.reconnecting) {
      return false;
    }
    this.reconnecting = true;
    this.noteDisconnected(now);
    return true;
  }

  noteDisconnected(now = new Date()): void {
    this.disconnectedSince ??= now;
  }

  markConnected(): void {
    this.reconnecting = false;
    this.disconnectedSince = null;
  }
}

export function rabbitMqClientsAreUnhealthy(
  clients: readonly RabbitMqDisconnectState[],
  now: Date,
  thresholdMs = RABBITMQ_DISCONNECT_UNHEALTHY_MS,
): boolean {
  return clients.some((client) => {
    if (!client.disconnectedSince) {
      return false;
    }
    return now.getTime() - client.disconnectedSince.getTime() > thresholdMs;
  });
}

export async function retireRabbitChannel(channel: Channel | undefined): Promise<void> {
  if (!channel) {
    return;
  }
  channel.removeAllListeners('error');
  channel.removeAllListeners('close');
  // Error events can arrive after retirement; keep them from becoming uncaught.
  channel.on('error', () => {});
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      channel.close(),
      new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, 5_000);
      }),
    ]);
  } catch {
    // Already closed by the broker.
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}
