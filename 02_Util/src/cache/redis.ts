// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { ICache } from '@citrineos/base';
import type { ClassConstructor } from 'class-transformer';
import { plainToInstance } from 'class-transformer';
import type {
  RedisClientOptions,
  RedisClientType,
  RedisFunctions,
  RedisModules,
  RedisScripts,
} from 'redis';
import { createClient } from 'redis';

/**
 * Implementation of cache interface with redis storage
 */
export class RedisCache implements ICache {
  private _client: RedisClientType<RedisModules, RedisFunctions, RedisScripts>;
  private _namespacePrefix?: string;

  constructor(clientOptions?: RedisClientOptions, namespacePrefix?: string) {
    this._namespacePrefix = namespacePrefix;
    this._client = clientOptions ? createClient(clientOptions) : createClient();
    this._client.on('connect', () => console.log('Redis client connected'));
    this._client.on('ready', () => console.log('Redis client ready to use'));
    this._client.on('error', (err) => console.error('Redis error', err));
    this._client.on('end', () => console.log('Redis client disconnected'));
    this._client
      .connect()
      .then()
      .catch((error) => {
        console.log('Error connecting to Redis', error);
      });
  }

  exists(key: string, namespace?: string): Promise<boolean> {
    key = this.prefixedKey(key, namespace);
    return this._client.exists(key).then((result) => result === 1);
  }

  remove(key: string, namespace?: string | undefined): Promise<boolean> {
    key = this.prefixedKey(key, namespace);
    return this._client.del(key).then((result) => result === 1);
  }

  onChange<T>(
    key: string,
    waitSeconds: number,
    namespace?: string | undefined,
    classConstructor?: (() => ClassConstructor<T>) | undefined,
  ): Promise<T | null> {
    const redisKey = this.prefixedKey(key, namespace);

    return new Promise((resolve) => {
      const subscriber = this._client.duplicate();
      let settled = false;
      const finish = (result: T | null | Promise<T | null>) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        this.closeSubscriber(subscriber);
        resolve(result);
      };

      // node-redis emits socket failures on 'error'. Without a listener that crashes the process.
      subscriber.on('error', () => undefined);

      const timer = setTimeout(() => {
        finish(this.get(key, namespace, classConstructor));
      }, waitSeconds * 1000);

      subscriber
        .connect()
        .then(() =>
          subscriber.subscribe(`__keyspace@0__:${redisKey}`, (event) => {
            if (event === 'set') {
              finish(this.get(key, namespace, classConstructor));
              return;
            }
            if (event === 'del' || event === 'expire') {
              finish(null);
            }
          }),
        )
        .catch(() => undefined);
    });
  }

  get<T>(
    key: string,
    namespace?: string,
    classConstructor?: () => ClassConstructor<T>,
  ): Promise<T | null> {
    key = this.prefixedKey(key, namespace);
    return this._client.get(key).then((result) => {
      if (result) {
        if (classConstructor) {
          return plainToInstance(classConstructor(), JSON.parse(result));
        }
        return result as T;
      }
      return null;
    });
  }

  set(key: string, value: string, namespace?: string, expireSeconds?: number): Promise<boolean> {
    key = this.prefixedKey(key, namespace);
    const setOptions = expireSeconds ? { EX: expireSeconds } : undefined;
    return this._client.set(key, value, setOptions).then((result) => {
      if (result) {
        return result === 'OK';
      }
      return false;
    });
  }

  setIfNotExist(
    key: string,
    value: string,
    namespace?: string,
    expireSeconds?: number,
  ): Promise<boolean> {
    key = this.prefixedKey(key, namespace);
    return this._client
      .set(key, value, expireSeconds ? { EX: expireSeconds, NX: true } : { NX: true })
      .then((result) => {
        if (result) {
          return result === 'OK';
        }
        return false;
      });
  }

  private closeSubscriber(subscriber: RedisClientType<RedisModules, RedisFunctions, RedisScripts>) {
    const disconnect = () => {
      subscriber.disconnect().catch(() => undefined);
    };
    if (!subscriber.isOpen) {
      disconnect();
      return;
    }
    subscriber.quit().catch(() => {
      disconnect();
    });
  }

  private prefixedKey(key: string, namespace?: string): string {
    const resolvedNamespace = namespace || 'default';
    const namespaceKey = `${resolvedNamespace}:${key}`;
    if (!this._namespacePrefix) {
      return namespaceKey;
    }
    return `${this._namespacePrefix}:${namespaceKey}`;
  }
}
