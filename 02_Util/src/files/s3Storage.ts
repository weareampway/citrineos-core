// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import {
  CreateBucketCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  assignMergedSystemConfig,
  type BootstrapConfig,
  type ConfigStore,
  type SystemConfig,
} from '@citrineos/base';
import { Readable } from 'stream';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import { commitConfigUpdate, ConfigVersionConflictError } from './commitConfigUpdate.js';
import { createWriteQueue } from './writeQueue.js';

export class S3Storage implements ConfigStore {
  protected readonly _logger: Logger<ILogObj>;
  private s3Client: S3Client;
  private defaultBucketName: string;
  private configFileName: string;
  private configBucketName: string | undefined;
  private readonly enqueue = createWriteQueue();

  constructor(
    config: BootstrapConfig['fileAccess']['s3'],
    configFileName: string,
    configDir?: string,
    logger?: Logger<ILogObj>,
  ) {
    this.s3Client = new S3Client({
      // Endpoint required for Minio
      ...(config!.endpoint ? { endpoint: config!.endpoint } : {}),
      // Region required for AWS S3
      ...(config!.region ? { region: config!.region } : {}),
      // Only set forcePathStyle to true for Minio, use default (false) for AWS S3
      forcePathStyle: !!config?.s3ForcePathStyle,
      // Add credentials if explicitly provided
      ...(config!.accessKeyId && config!.secretAccessKey
        ? {
            credentials: {
              accessKeyId: config!.accessKeyId,
              secretAccessKey: config!.secretAccessKey,
            },
          }
        : {}),
    });
    this.defaultBucketName = config!.defaultBucketName!;
    this.configFileName = configFileName!;
    this.configBucketName = configDir;
    this._logger = logger
      ? logger.getSubLogger({ name: this.constructor.name })
      : new Logger<ILogObj>({ name: this.constructor.name });
  }
  async saveFile(fileName: string, content: Buffer, filePath?: string): Promise<string> {
    const bucketName = filePath ? filePath : this.defaultBucketName;
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: fileName,
      Body: content,
      ContentType: 'application/octet-stream',
    });
    try {
      const result = await this.s3Client.send(command);

      if (result.$metadata.httpStatusCode !== 200) {
        throw new Error(`Failed to upload file ${fileName}: ${result.$metadata.httpStatusCode}`);
      } else {
        return fileName;
      }
    } catch (error: any) {
      if (error.name === 'NoSuchBucket' || error.$metadata?.httpStatusCode === 404) {
        this._logger.warn(`Bucket "${bucketName}" not found. Creating it...`);
        await this.createBucket(bucketName);
        this._logger.info(`Bucket "${bucketName}" created. Retrying config save...`);
        return await this.saveFile(fileName, content, filePath);
      } else {
        this._logger.error('Error saving config to S3:', error);
        throw error;
      }
    }
  }

  async getFile(id: string, filePath?: string): Promise<string | undefined> {
    const command = new GetObjectCommand({
      Bucket: filePath ? filePath : this.defaultBucketName,
      Key: id,
    });
    const { Body } = await this.s3Client.send(command);

    if (!Body) return;

    return await S3Storage.streamToString(Body as Readable);
  }

  async fetchConfig(): Promise<SystemConfig | null> {
    const loaded = await this.fetchVersionedConfig();
    return loaded?.config ?? null;
  }

  async updateConfig(mutate: (config: SystemConfig) => void): Promise<SystemConfig> {
    const updated = await this.enqueue(() =>
      commitConfigUpdate(
        () => this.fetchVersionedConfig(),
        (config, version) => this.putConfig(config, { ifMatch: version }),
        mutate,
      ),
    );
    this._logger.info('Config saved to S3.');
    return updated;
  }

  async saveConfigIfAbsent(config: SystemConfig): Promise<boolean> {
    return this.enqueue(async () => {
      try {
        await this.putConfig(config, { ifNoneMatch: '*' });
        this._logger.info('Config saved to S3.');
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

  private async createBucket(bucket: string): Promise<void> {
    try {
      const command = new CreateBucketCommand({ Bucket: bucket });
      await this.s3Client.send(command);
      this._logger.info(`Bucket "${bucket}" created successfully.`);
    } catch (error) {
      this._logger.error(`Failed to create bucket "${bucket}":`, error);
      throw error;
    }
  }

  private async fetchVersionedConfig(): Promise<{ config: SystemConfig; version: string } | null> {
    const command = new GetObjectCommand({
      Bucket: this.configBucket(),
      Key: this.configFileName,
    });
    try {
      const result = await this.s3Client.send(command);
      if (!result.Body) return null;
      if (!result.ETag) {
        throw new Error('S3 config object is missing an ETag');
      }
      const configString = await S3Storage.streamToString(result.Body as Readable);
      return { config: JSON.parse(configString) as SystemConfig, version: result.ETag };
    } catch (error: any) {
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        this._logger.warn('Config not found in S3.');
        return null;
      }
      this._logger.error('Error fetching config from S3:', error);
      throw error;
    }
  }

  private async putConfig(
    config: SystemConfig,
    condition: { ifMatch?: string; ifNoneMatch?: string },
  ): Promise<void> {
    const bucketName = this.configBucket();
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: this.configFileName,
      Body: Buffer.from(JSON.stringify(config, null, 2)),
      ContentType: 'application/octet-stream',
      ...(condition.ifMatch ? { IfMatch: condition.ifMatch } : {}),
      ...(condition.ifNoneMatch ? { IfNoneMatch: condition.ifNoneMatch } : {}),
    });
    try {
      const result = await this.s3Client.send(command);
      if (result.$metadata.httpStatusCode !== 200) {
        throw new Error(
          `Failed to upload file ${this.configFileName}: ${result.$metadata.httpStatusCode}`,
        );
      }
    } catch (error: any) {
      if (error instanceof ConfigVersionConflictError) {
        throw error;
      }
      if (this.isConditionalConflict(error)) {
        throw new ConfigVersionConflictError();
      }
      if (error.name === 'NoSuchBucket') {
        this._logger.warn(`Bucket "${bucketName}" not found. Creating it...`);
        await this.createBucket(bucketName);
        this._logger.info(`Bucket "${bucketName}" created. Retrying config save...`);
        return this.putConfig(config, condition);
      }
      this._logger.error('Error saving config to S3:', error);
      throw error;
    }
  }

  private configBucket(): string {
    return this.configBucketName ? this.configBucketName : this.defaultBucketName;
  }

  private isConditionalConflict(error: { $metadata?: { httpStatusCode?: number } }): boolean {
    const status = error.$metadata?.httpStatusCode;
    return status === 412 || status === 409;
  }

  private static async streamToString(stream: Readable): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Uint8Array[] = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      stream.on('error', reject);
    });
  }
}
