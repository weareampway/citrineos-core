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
  private _subscriber?: RedisClientType<RedisModules, RedisFunctions, RedisScripts>;

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
    const channel = `__keyspace@${this._client.options?.database ?? 0}__:${redisKey}`;
    const subscriber = this.subscriber();
    // Read before subscribing and compared once the subscription is live, so a change made
    // while SUBSCRIBE is in flight is not missed. undefined means the read failed.
    const valueBefore = this._client.get(redisKey).catch(() => undefined);

    return new Promise((resolve) => {
      let settled = false;
      const finish = (result: T | null | Promise<T | null>) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        // Unsubscribe only after SUBSCRIBE has completed, otherwise its late reply re-adds the listener.
        subscribed
          .then((isSubscribed) =>
            isSubscribed ? subscriber.unsubscribe(channel, listener) : undefined,
          )
          .catch((error) => {
            console.warn('Redis keyspace unsubscribe failed', channel, error);
          });
        resolve(result);
      };

      // Message is the command run on the key. 'expire' only means a TTL was set (SET ... EX
      // sends it right after 'set'); a key that times out sends 'expired'.
      const listener = (event: string) => {
        if (event === 'set') {
          finish(this.get(key, namespace, classConstructor));
          return;
        }
        if (event === 'del' || event === 'expired' || event === 'evicted') {
          finish(null);
        }
      };

      const timer = setTimeout(() => {
        finish(this.get(key, namespace, classConstructor));
      }, waitSeconds * 1000);

      const subscribed = subscriber.subscribe(channel, listener).then(
        () => true,
        (error) => {
          console.warn('Redis keyspace subscribe failed', channel, error);
          return false;
        },
      );

      subscribed
        .then(async (isSubscribed) => {
          const before = await valueBefore;
          if (!isSubscribed || settled || before === undefined) {
            return;
          }
          const current = await this._client.get(redisKey);
          if (current !== before) {
            finish(this.get(key, namespace, classConstructor));
          }
        })
        .catch((error) => {
          console.warn('Redis keyspace re-check failed', channel, error);
        });
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

  /**
   * One long-lived connection for all keyspace subscriptions. node-redis keeps a listener set per
   * channel and resubscribes after a reconnect, so waits only SUBSCRIBE and UNSUBSCRIBE on it.
   */
  private subscriber(): RedisClientType<RedisModules, RedisFunctions, RedisScripts> {
    if (!this._subscriber) {
      const subscriber = this._client.duplicate();
      // node-redis emits socket failures on 'error'. Without a listener that crashes the process.
      subscriber.on('error', (error) => console.warn('Redis subscriber error', error));
      subscriber.connect().catch((error) => {
        console.warn('Error connecting Redis subscriber', error);
      });
      this._subscriber = subscriber;
    }
    return this._subscriber;
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
