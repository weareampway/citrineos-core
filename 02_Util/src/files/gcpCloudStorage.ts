// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import {
  assignMergedSystemConfig,
  type BootstrapConfig,
  type ConfigStore,
  type SystemConfig,
} from '@citrineos/base';
import { Bucket, Storage } from '@google-cloud/storage';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import { commitConfigUpdate, ConfigVersionConflictError } from './commitConfigUpdate.js';
import { createWriteQueue } from './writeQueue.js';

export class GcpCloudStorage implements ConfigStore {
  protected readonly _logger: Logger<ILogObj>;
  private storageClient: Storage;
  private configBucketName: string;
  private configFileName: string;
  private readonly enqueue = createWriteQueue();

  constructor(
    config: BootstrapConfig['fileAccess']['gcp'],
    configFileName: string,
    configDir?: string,
    logger?: Logger<ILogObj>,
  ) {
    if (!config) {
      throw new Error('GCP Cloud Storage config missing.');
    }
    this.storageClient = new Storage({
      projectId: config.projectId,
      credentials: config.credentials,
    });
    this.configBucketName = configDir || 'default';
    this.configFileName = configFileName;
    this._logger = logger
      ? logger.getSubLogger({ name: this.constructor.name })
      : new Logger<ILogObj>({ name: this.constructor.name });
  }

  /**
   * Save a raw file buffer into GCS.
   *
   * @param fileName - Object key / blob name.
   * @param content  - File data.
   * @param filePath - Optional bucket name, falls back to configBucketName.
   */
  async saveFile(fileName: string, content: Buffer, filePath?: string): Promise<string> {
    const bucketName = filePath ? filePath : this.configBucketName;
    const bucket = this.getBucket(bucketName);
    const file = bucket.file(fileName);

    try {
      await file.save(content, {
        contentType: 'application/octet-stream',
        resumable: false,
      });
      return fileName;
    } catch (error: any) {
      if (this.isNotFoundError(error)) {
        this._logger.warn(`Bucket "${bucketName}" not found. Creating it...`);
        await this.createBucket(bucketName);
        this._logger.info(`Bucket "${bucketName}" created. Retrying file save...`);
        return this.saveFile(fileName, content, filePath);
      }

      this._logger.error('Error saving file to GCP Cloud Storage:', error);
      throw error;
    }
  }

  /**
   * Read a file from GCS and return its contents as UTF-8 string.
   *
   * @param id       - Object key / blob name.
   * @param filePath - Optional bucket name, falls back to configBucketName.
   */
  async getFile(id: string, filePath?: string): Promise<string | undefined> {
    const bucketName = filePath ? filePath : this.configBucketName;
    const bucket = this.getBucket(bucketName);
    const file = bucket.file(id);

    try {
      const [exists] = await file.exists();
      if (!exists) return;

      const [contents] = await file.download();
      return contents.toString('utf-8');
    } catch (error: any) {
      if (this.isNotFoundError(error)) {
        // Treat missing file like S3's NoSuchKey
        return;
      }
      this._logger.error('Error reading file from GCP Cloud Storage:', error);
      throw error;
    }
  }

  async fetchConfig(): Promise<SystemConfig | null> {
    const loaded = await this.fetchVersionedConfig();
    return loaded?.config ?? null;
  }

  async updateConfig(mutate: (config: SystemConfig) => void): Promise<SystemConfig> {
    const updated = await this.enqueue(() =>
      commitConfigUpdate(
        () => this.fetchVersionedConfig(),
        (config, version) => this.putConfig(config, { ifGenerationMatch: version }),
        mutate,
      ),
    );
    this._logger.info('Config saved to GCP Cloud Storage.');
    return updated;
  }

  async saveConfigIfAbsent(config: SystemConfig): Promise<boolean> {
    return this.enqueue(async () => {
      try {
        await this.putConfig(config, { ifGenerationMatch: 0 });
        this._logger.info('Config saved to GCP Cloud Storage.');
        return true;
      } catch (error) {
        if (error instanceof ConfigVersionConflictError) {
          return false;
        }
        throw error;
      }
    });
  }

  async saveConfig(config: SystemConfig): Promise<void> {
    await this.updateConfig((latest) => {
      assignMergedSystemConfig(latest, config);
    });
  }

  private async fetchVersionedConfig(): Promise<{ config: SystemConfig; version: string } | null> {
    const bucket = this.getBucket(this.configBucketName);
    const readAttempts = 5;

    for (let attempt = 0; attempt < readAttempts; attempt++) {
      const file = bucket.file(this.configFileName);
      let generation: string;
      try {
        const [metadata] = await file.getMetadata();
        if (metadata.generation === undefined || metadata.generation === null) {
          throw new Error('GCS config object is missing a generation');
        }
        generation = String(metadata.generation);
      } catch (error: any) {
        if (this.isNotFoundError(error)) {
          this._logger.warn('Config not found in GCP Cloud Storage.');
          return null;
        }
        this._logger.error('Error fetching config from GCP Cloud Storage:', error);
        throw error;
      }

      try {
        const pinned = bucket.file(this.configFileName, { generation });
        const [contents] = await pinned.download();
        return {
          config: JSON.parse(contents.toString('utf-8')) as SystemConfig,
          version: generation,
        };
      } catch (error: any) {
        if (this.isNotFoundError(error) && attempt < readAttempts - 1) {
          continue;
        }
        this._logger.error('Error fetching config from GCP Cloud Storage:', error);
        throw error;
      }
    }

    throw new Error('GCS config generation changed while reading');
  }

  private async putConfig(
    config: SystemConfig,
    precondition: { ifGenerationMatch: number | string },
  ): Promise<void> {
    const bucketName = this.configBucketName;
    const file = this.getBucket(bucketName).file(this.configFileName);
    try {
      await file.save(Buffer.from(JSON.stringify(config, null, 2)), {
        contentType: 'application/octet-stream',
        resumable: false,
        preconditionOpts: precondition,
      });
    } catch (error: any) {
      if (error instanceof ConfigVersionConflictError) {
        throw error;
      }
      if (this.isPreconditionFailure(error)) {
        throw new ConfigVersionConflictError();
      }
      if (this.isNotFoundError(error)) {
        this._logger.warn(`Bucket "${bucketName}" not found. Creating it...`);
        await this.createBucket(bucketName);
        this._logger.info(`Bucket "${bucketName}" created. Retrying config save...`);
        return this.putConfig(config, precondition);
      }
      this._logger.error('Error saving config to GCP Cloud Storage:', error);
      throw error;
    }
  }

  private isPreconditionFailure(error: { code?: number }): boolean {
    return error?.code === 412 || error?.code === 409;
  }

  private getBucket(name: string): Bucket {
    return this.storageClient.bucket(name);
  }

  private async createBucket(bucketName: string): Promise<void> {
    try {
      await this.storageClient.createBucket(bucketName);
      this._logger.info(`Bucket "${bucketName}" created successfully.`);
    } catch (error) {
      this._logger.error(`Failed to create bucket "${bucketName}" in GCP Cloud Storage:`, error);
      throw error;
    }
  }

  /**
   * Normalize "not found" checks across GCS error shapes.
   */
  private isNotFoundError(error: any): boolean {
    return (
      error?.code === 404 ||
      (typeof error?.message === 'string' &&
        (error.message.includes('No such object') ||
          error.message.includes('Not Found') ||
          error.message.includes('could not find')))
    );
  }
}
