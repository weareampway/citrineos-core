// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import type { CallAction } from '@citrineos/base';
import { RabbitMqReceiver } from './receiver.js';

interface StationChannel {
  receiver: RabbitMqReceiver;
  ready: Promise<void>;
}

/**
 * The router registers a request and a response consumer per station. Give each station
 * a channel on the shared AMQP connection so a channel closure only interrupts that station.
 * The inherited connection checks broker health even before the first station connects.
 */
export class RabbitMqStationReceiver extends RabbitMqReceiver {
  private readonly stations = new Map<string, StationChannel>();
  private shuttingDown = false;

  override get disconnectedSince(): Date | null {
    let earliest = super.disconnectedSince;
    for (const { receiver } of this.stations.values()) {
      const since = receiver.disconnectedSince;
      if (since && (!earliest || since < earliest)) earliest = since;
    }
    return earliest;
  }

  override async subscribe(
    identifier: string,
    actions?: CallAction[],
    filter?: { [k: string]: string },
  ): Promise<boolean> {
    if (this.shuttingDown) return false;
    let station = this.stations.get(identifier);
    if (!station) {
      const receiver = new RabbitMqReceiver(this._config, this._logger, this.module);
      // A new WebSocket must finish or fail registration so its caller can clean up.
      // Once connected, the receiver still recovers its consumers in the background.
      station = { receiver, ready: receiver.initConnection({ retry: false }) };
      this.stations.set(identifier, station);
    }

    try {
      await station.ready;
      if (this.stations.get(identifier) !== station) return false;
      const subscribed = await station.receiver.subscribe(identifier, actions, filter);
      return subscribed && this.stations.get(identifier) === station;
    } catch (error) {
      // Either subscription failing invalidates this registration. A reconnect may
      // already own a replacement channel by the time the old subscribe rejects.
      if (this.stations.get(identifier) === station) await this.unsubscribe(identifier);
      throw error;
    }
  }

  override async unsubscribe(identifier: string): Promise<boolean> {
    const station = this.stations.get(identifier);
    if (station) {
      this.stations.delete(identifier);
      // Closing this station's channel cancels both consumers, including an in-flight consume.
      await station.receiver.shutdown();
    }
    return true;
  }

  override async shutdown(): Promise<void> {
    this.shuttingDown = true;
    const stations = [...this.stations.keys()];
    await Promise.all([super.shutdown(), ...stations.map((id) => this.unsubscribe(id))]);
  }
}
