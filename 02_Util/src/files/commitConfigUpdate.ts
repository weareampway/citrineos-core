// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { SystemConfig } from '@citrineos/base';

const CONFIG_UPDATE_ATTEMPTS = 5;

export class ConfigVersionConflictError extends Error {
  constructor() {
    super('Config was modified concurrently');
    this.name = 'ConfigVersionConflictError';
  }
}

export interface VersionedConfig {
  config: SystemConfig;
  version: string;
}

export async function commitConfigUpdate(
  load: () => Promise<VersionedConfig | null>,
  save: (config: SystemConfig, version: string) => Promise<void>,
  mutate: (config: SystemConfig) => void,
): Promise<SystemConfig> {
  let conflict: ConfigVersionConflictError | undefined;

  for (let attempt = 0; attempt < CONFIG_UPDATE_ATTEMPTS; attempt++) {
    const loaded = await load();
    if (!loaded) {
      throw new Error('No configuration found in storage');
    }

    const draft = structuredClone(loaded.config);
    mutate(draft);

    try {
      await save(draft, loaded.version);
      return draft;
    } catch (error) {
      if (error instanceof ConfigVersionConflictError) {
        conflict = error;
        continue;
      }
      throw error;
    }
  }

  throw conflict ?? new ConfigVersionConflictError();
}
