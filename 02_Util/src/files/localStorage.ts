// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import fs from 'fs';
import path from 'path';
import type { ConfigStore, SystemConfig } from '@citrineos/base';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import { commitConfigUpdate, ConfigVersionConflictError } from './commitConfigUpdate.js';

export class LocalStorage implements ConfigStore {
  protected readonly _logger: Logger<ILogObj>;
  private defaultFilePath: string;
  private configFileName: string;
  private configDir: string | undefined;
  private pendingWrites: Promise<void> = Promise.resolve();
  private generation = 0;

  constructor(
    defaultFilePath: string,
    configFileName: string,
    configDir?: string,
    logger?: Logger<ILogObj>,
  ) {
    this.defaultFilePath = defaultFilePath;
    this.configFileName = configFileName;
    this.configDir = configDir;
    this._logger = logger
      ? logger.getSubLogger({ name: this.constructor.name })
      : new Logger<ILogObj>({ name: this.constructor.name });
  }

  async saveFile(fileName: string, content: Buffer, filePath?: string): Promise<string> {
    const absoluteFilePath = path.join(
      process.cwd(),
      filePath ? filePath : this.defaultFilePath,
      fileName,
    );
    this._logger.debug(`Saving file to ${absoluteFilePath}`);
    fs.writeFileSync(absoluteFilePath, content, 'utf-8');
    return absoluteFilePath;
  }

  async getFile(id: string, filePath?: string): Promise<string | undefined> {
    const absoluteFilePath = path.join(
      process.cwd(),
      filePath ? filePath : this.defaultFilePath,
      id,
    );
    this._logger.debug(`Getting file from ${absoluteFilePath}`);
    if (!fs.existsSync(absoluteFilePath)) {
      return;
    }
    return fs.readFileSync(absoluteFilePath, 'utf-8');
  }

  async fetchConfig(): Promise<SystemConfig | null> {
    try {
      const configString = await this.getFile(this.configFileName, this.configDir);
      if (!configString) return null;
      return JSON.parse(configString) as SystemConfig;
    } catch (error) {
      this._logger.error('Error fetching config from local storage:', error);
      return null;
    }
  }

  async updateConfig(mutate: (config: SystemConfig) => void): Promise<SystemConfig> {
    return this.enqueue(() =>
      commitConfigUpdate(
        async () => {
          const config = await this.fetchConfig();
          if (!config) return null;
          return { config, version: String(this.generation) };
        },
        async (config, version) => {
          if (version !== String(this.generation)) {
            throw new ConfigVersionConflictError();
          }
          await this.writeConfig(config);
        },
        mutate,
      ),
    );
  }

  async saveConfigIfAbsent(config: SystemConfig): Promise<boolean> {
    return this.enqueue(async () => {
      const existing = await this.fetchConfig();
      if (existing) return false;
      await this.writeConfig(config);
      this._logger.info('Config saved locally.');
      return true;
    });
  }

  async saveConfig(config: SystemConfig): Promise<void> {
    try {
      await this.enqueue(() => this.writeConfig(config));
      this._logger.info('Config saved locally.');
    } catch (error) {
      this._logger.error('Error saving config to local storage:', error);
    }
  }

  private enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.pendingWrites.then(operation);
    this.pendingWrites = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async writeConfig(config: SystemConfig): Promise<void> {
    await this.saveFile(
      this.configFileName,
      Buffer.from(JSON.stringify(config, null, 2)),
      this.configDir,
    );
    this.generation += 1;
  }
}
