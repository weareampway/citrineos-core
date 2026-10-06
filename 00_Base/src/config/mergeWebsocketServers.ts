// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { SystemConfig, WebsocketServerConfig } from './types.js';

export function mergeWebsocketServers(
  stored: WebsocketServerConfig[],
  requested: WebsocketServerConfig[],
): WebsocketServerConfig[] {
  const requestedById = new Map(requested.map((server) => [server.id, server]));
  return stored.map((server) => {
    const update = requestedById.get(server.id);
    return update ? structuredClone(update) : server;
  });
}

export function assignMergedSystemConfig(latest: SystemConfig, incoming: SystemConfig): void {
  const websocketServers = mergeWebsocketServers(
    latest.util.networkConnection.websocketServers,
    incoming.util.networkConnection.websocketServers,
  );
  const replacement = structuredClone(incoming);
  replacement.util.networkConnection.websocketServers = websocketServers;
  Object.assign(latest, replacement);
}
