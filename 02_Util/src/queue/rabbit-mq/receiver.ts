// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { CallAction, CircuitBreakerState, IModule, SystemConfig } from '@citrineos/base';
import {
  AbstractMessageHandler,
  CircuitBreaker,
  Message,
  RetryMessageError,
} from '@citrineos/base';
import * as amqplib from 'amqplib';
import type { ILogObj } from 'tslog';
import { RabbitMqConnectionHealth, retireRabbitChannel } from './connection-health.js';
import { getSharedChannel } from './shared-connection-pool.js';
import { Logger } from 'tslog';

/**
 * Implementation of a {@link IMessageHandler} using RabbitMQ as the underlying transport.
 */
type SubscriptionRegistration = {
  identifier: string;
  actions?: CallAction[];
  filter?: { [k: string]: string };
  /** The consumer this process created, with the channel it lives on. */
  consumer?: { channel: amqplib.Channel; tag: string };
  /** Settles when the subscribe that created this registration has finished. */
  ready?: Promise<boolean>;
};

function isSameSubscription(
  sub: SubscriptionRegistration,
  identifier: string,
  actions?: CallAction[],
  filter?: { [k: string]: string },
): boolean {
  return (
    sub.identifier === identifier &&
    JSON.stringify(sub.actions) === JSON.stringify(actions) &&
    JSON.stringify(sub.filter) === JSON.stringify(filter)
  );
}

export class RabbitMqReceiver extends AbstractMessageHandler {
  /**
   * Constants
   */
  private static readonly DEFAULT_QUEUE_PREFIX = 'rabbit_queue_';
  private static readonly RECONNECT_DELAY = 5000;
  /**
   * Station queues are shared by every instance the station may connect to. They
   * expire after this long without consumers instead of being deleted with the last
   * one: cancelling it while another subscribe has already declared the queue would
   * make that subscribe's bind or consume fail with a 404, which closes its channel.
   */
  private static readonly STATION_QUEUE_EXPIRES_MS = 30_000;
  /**
   * RabbitMQ rejects redeclaring a queue with other arguments (406, closes the
   * channel), so expiring station queues must not share names with the autoDelete
   * queues that older instances still declare during a rolling deploy.
   */
  private static readonly STATION_QUEUE_INFIX = 'exp_';

  /**
   * Fields
   */
  protected _channel?: amqplib.Channel;
  private _connectionHealth = new RabbitMqConnectionHealth();
  private _connectionAttempt?: Promise<amqplib.Channel>;
  private _channelRetirement: Promise<void> = Promise.resolve();
  private _abortReconnectController = new AbortController();
  private _stopped = false;
  private _circuitBreaker: CircuitBreaker;
  private _reconnectInterval?: NodeJS.Timeout;
  private _subscriptionRegistry: SubscriptionRegistration[] = [];

  constructor(
    config: SystemConfig,
    logger?: Logger<ILogObj>,
    module?: IModule,
    circuitBreaker?: CircuitBreaker,
  ) {
    super(config, logger, module);
    this._circuitBreaker = circuitBreaker ?? new CircuitBreaker();
    this._circuitBreaker.onStateChange(this._onCircuitBreakerStateChange.bind(this));
  }

  get disconnectedSince(): Date | null {
    return this._connectionHealth.disconnectedSince;
  }

  async initConnection(): Promise<any> {
    if (this._circuitBreaker.state === 'CLOSED') {
      throw new Error('Circuit breaker is CLOSED. Cannot initialize RabbitMQ connection.');
    }
    await this._connectWithRetry(this._abortReconnectController.signal);
  }

  /**
   * Methods
   */

  /**
   * Binds queue to an exchange given identifier and optional actions and filter.
   * Note: Due to the nature of AMQP 0-9-1 model, if you need to filter for the identifier, you **MUST** provide it in the filter object.
   *
   * @param {string} identifier - The identifier of the channel to subscribe to.
   * @param {CallAction[]} actions - Optional. An array of actions to filter the messages.
   * @param {{ [k: string]: string; }} filter - Optional. An object representing the filter to apply on the messages.
   * @return {Promise<boolean>} A promise that resolves to true if the subscription is successful, false otherwise.
   */
  async subscribe(
    identifier: string,
    actions?: CallAction[],
    filter?: { [k: string]: string },
  ): Promise<boolean> {
    // If actions are a defined but empty list, it is likely a module
    // with no available actions and should not have a queue.
    //
    // If actions are undefined, it is likely a charger,
    // which is "allowed" not to have actions.
    if (actions && actions.length === 0) {
      this._logger.debug(
        `Skipping queue binding for module ${identifier} as there are no available actions.`,
      );

      return true;
    }

    // A station that reconnects while its old socket is still open subscribes again
    // without being deregistered; reuse the registration instead of adding a consumer.
    const existing = this._subscriptionRegistry.find((s) =>
      isSameSubscription(s, identifier, actions, filter),
    );
    if (existing) {
      return existing.ready!;
    }

    // Registered before subscribing, so an unsubscribe that runs meanwhile sees it.
    const sub: SubscriptionRegistration = { identifier, actions, filter };
    this._subscriptionRegistry.push(sub);
    sub.ready = this._performSubscribe(sub).catch(async (err) => {
      this._subscriptionRegistry = this._subscriptionRegistry.filter((s) => s !== sub);
      // A resubscribe on a newer channel may have created a consumer meanwhile.
      await this._cancelConsumer(sub);
      throw err;
    });
    return sub.ready;
  }

  /**
   * Cancels only the consumers this process created. Deleting the queue would also
   * cancel the consumers of every other instance the station has moved to, and a
   * second delete of the same name is a 404 that closes the channel.
   *
   * @return false if a consumer could not be cancelled.
   */
  async unsubscribe(identifier: string): Promise<boolean> {
    const removed = this._subscriptionRegistry.filter((s) => s.identifier === identifier);
    this._subscriptionRegistry = this._subscriptionRegistry.filter(
      (s) => s.identifier !== identifier,
    );
    let ok = true;
    for (const sub of removed) {
      if (!(await this._cancelConsumer(sub))) ok = false;
    }
    return ok;
  }

  async shutdown(): Promise<void> {
    this._stopped = true;
    this._abortReconnectController.abort();
    if (this._reconnectInterval) clearInterval(this._reconnectInterval);
    this._reconnectInterval = undefined;
    this._subscriptionRegistry = [];
    const channel = this._channel;
    this._channel = undefined;
    this._connectionHealth.noteDisconnected();
    await retireRabbitChannel(channel);
  }

  /**
   * Protected Methods
   */

  /**
   * Connect to RabbitMQ with retry logic.
   * This method will keep trying to connect until successful, unless aborted.
   *
   * @param {AbortSignal} [abortSignal] - Optional abort signal to stop retrying.
   * @return {Promise<amqplib.Channel>} A promise that resolves to the AMQP channel.
   */
  protected _connectWithRetry(
    abortSignal = this._abortReconnectController.signal,
  ): Promise<amqplib.Channel> {
    // Initial connection, interval retries and breaker callbacks share one attempt.
    this._connectionAttempt ??= this._connectAndSubscribeWithRetry(abortSignal).finally(() => {
      this._connectionAttempt = undefined;
    });
    return this._connectionAttempt;
  }

  private async _connectAndSubscribeWithRetry(abortSignal?: AbortSignal): Promise<amqplib.Channel> {
    const url = this._config.util.messageBroker.amqp?.url;
    if (!url) {
      throw new Error('RabbitMQ URL is not configured');
    }
    while (true) {
      if (this._stopped || abortSignal?.aborted) {
        this._logger.warn('RabbitMQ reconnect aborted by signal.');
        throw new Error('RabbitMQ reconnect aborted');
      }
      if (this._circuitBreaker.state === 'CLOSED') {
        throw new Error('Circuit breaker is CLOSED. Cannot connect to RabbitMQ.');
      }
      try {
        await this._channelRetirement;
        await retireRabbitChannel(this._channel);
        this._channel = undefined;
        const channel = await getSharedChannel(url, this._logger);
        if (this._stopped || abortSignal?.aborted) {
          await retireRabbitChannel(channel);
          throw new Error('RabbitMQ reconnect aborted');
        }
        channel.on('error', (err) => {
          if (this._channel === channel) {
            this._logger.error('AMQP channel error', err);
            this._handleDisconnect();
          }
        });
        channel.on('close', () => {
          if (this._channel === channel) this._handleDisconnect();
        });
        this._channel = channel;
        await this._resubscribeAll();
        if (this._channel !== channel || this._stopped || abortSignal?.aborted) {
          throw new Error('RabbitMQ channel closed while restoring subscriptions.');
        }
        this._connectionHealth.markConnected();
        this._circuitBreaker.triggerSuccess();
        return channel;
      } catch (err) {
        if (this._stopped || abortSignal?.aborted) throw err;
        this._logger.error('RabbitMQ connect failed, triggering circuit breaker failure', err);
        this._connectionHealth.noteDisconnected();
        this._circuitBreaker.triggerFailure((err as Error)?.message);
        // Wait for circuit breaker to allow retry (exponential backoff handled by circuit breaker)
        await new Promise((res) => setTimeout(res, 1000));
      }
    }
  }

  /**
   * Underlying RabbitMQ message handler.
   *
   * @param message The AMQPMessage to process
   * @param channel
   */
  protected async _onMessage(
    message: amqplib.ConsumeMessage | null,
    channel: amqplib.Channel,
  ): Promise<void> {
    if (message) {
      try {
        this._logger.debug(
          '_onMessage:Received message:',
          message.properties,
          message.content.toString(),
        );
        const messageData = JSON.parse(message.content.toString());

        // Create Message instance with generic payload (no type transformation needed)
        const parsed = new Message(
          messageData.origin || messageData._origin,
          messageData.eventGroup || messageData._eventGroup,
          messageData.action || messageData._action,
          messageData.state || messageData._state,
          messageData.context || messageData._context,
          messageData.payload || messageData._payload, // Keep payload as generic object
          messageData.protocol || messageData._protocol,
        );
        await this.handle(parsed, message.properties);
      } catch (error) {
        if (error instanceof RetryMessageError) {
          this._logger.warn('Retrying message: ', error.message);
          // Retryable error, usually ongoing call with station when trying to send new call
          channel.nack(message);
          return;
        } else {
          this._logger.error('Error while processing message:', error, message);
        }
      }
      channel.ack(message);
    }
  }

  private _startReconnectInterval() {
    if (this._stopped) return;
    if (this._reconnectInterval) {
      clearInterval(this._reconnectInterval);
    }
    const delay = (this._config.maxReconnectDelay || 30) * 1000;
    this._logger.warn(
      `Starting continuous reconnect attempts every ${delay / 1000} seconds while circuit breaker is FAILING.`,
    );
    this._reconnectInterval = setInterval(() => {
      this._logger.info('Attempting RabbitMQ reconnect due to circuit breaker FAILING...');
      this._connectWithRetry()
        .then(() => {
          this._logger.info('RabbitMQ reconnect attempt succeeded.');
        })
        .catch((err) => {
          this._logger.error('RabbitMQ reconnect attempt failed.', err);
        });
    }, delay);
  }

  private _onCircuitBreakerStateChange(state: CircuitBreakerState, reason?: string) {
    if (this._stopped) return;
    this._logger.info(`[CircuitBreaker] State changed to ${state}${reason ? `: ${reason}` : ''}`);

    switch (state) {
      case 'CLOSED': {
        this._logger.error(
          'Circuit breaker CLOSED: shutting down RabbitMQ receiver. Reason:',
          reason,
        );
        void this.shutdown();
        if (this._reconnectInterval) {
          this._logger.info('Clearing reconnect interval as circuit breaker is now CLOSED.');
          clearInterval(this._reconnectInterval);
          this._reconnectInterval = undefined;
        }
        break;
      }
      case 'OPEN': {
        this._logger.info('Circuit breaker is OPEN.');
        if (this._reconnectInterval) {
          this._logger.info('Clearing reconnect interval as circuit breaker is now OPEN.');
          clearInterval(this._reconnectInterval);
          this._reconnectInterval = undefined;
        }
        if (!this._channel) {
          this._logger.info(
            'RabbitMQ channel missing while OPEN. Attempting to (re)initialize connection.',
          );
          this._connectWithRetry()
            .then(() => {
              this._logger.info('RabbitMQ connection (re)initialized.');
            })
            .catch((err) => {
              this._logger.error('RabbitMQ (re)init failed.', err);
            });
        }
        break;
      }
      case 'FAILING': {
        this._logger.warn(
          'Circuit breaker is FAILING. RabbitMQ receiver will not receive messages until recovery. Reason:',
          reason,
        );
        this._logger.info('Attempting to start reconnect interval after circuit breaker FAILING.');
        this._startReconnectInterval();
        break;
      }
      default:
        this._logger.warn('Unknown circuit breaker state:', state);
        break;
    }
  }

  /**
   * Handle RabbitMQ disconnection.
   * This method will attempt to reconnect to RabbitMQ when the connection is lost.
   * Debounces concurrent reconnects.
   */
  private _handleDisconnect() {
    const firstDisconnect = this._connectionHealth.beginDisconnect();
    const previous = this._channel;
    this._channel = undefined;
    this._channelRetirement = retireRabbitChannel(previous);
    if (!firstDisconnect) {
      return;
    }
    this._logger.warn('RabbitMQ connection lost. Triggering circuit breaker failure.');
    this._circuitBreaker.triggerFailure('RabbitMQ connection lost');
    if (!this._stopped && this._circuitBreaker.state !== 'CLOSED') {
      void this._connectWithRetry().catch((err) => {
        if (!this._stopped) this._logger.error('RabbitMQ recovery failed', err);
      });
    }
  }

  private async _performSubscribe(sub: SubscriptionRegistration): Promise<boolean> {
    const { identifier, actions, filter } = sub;
    const exchange = this._config.util.messageBroker.amqp?.exchange as string;
    const queuePrefix =
      (this._config.util.messageBroker.amqp as { queuePrefix?: string } | undefined)?.queuePrefix ??
      RabbitMqReceiver.DEFAULT_QUEUE_PREFIX;
    // Stations subscribe without actions (see subscribe). Module queues keep autoDelete:
    // renamed, every module message would reach old and new instances during a deploy.
    const stationQueue = !actions;
    const queueName = stationQueue
      ? `${queuePrefix}${RabbitMqReceiver.STATION_QUEUE_INFIX}${identifier}`
      : `${queuePrefix}${identifier}`;

    const filterWithMatch = filter
      ? {
          'x-match': 'all',
          ...filter,
        }
      : { 'x-match': 'all' };

    if (!this._channel) {
      throw new Error('RabbitMQ is down: cannot subscribe.');
    }
    const channel = this._channel;

    await channel.assertExchange(exchange, 'headers', { durable: false });
    await channel.assertQueue(
      queueName,
      stationQueue
        ? { durable: false, exclusive: false, expires: RabbitMqReceiver.STATION_QUEUE_EXPIRES_MS }
        : { durable: false, autoDelete: true, exclusive: false },
    );

    if (actions && actions.length > 0) {
      for (const action of actions) {
        this._logger.debug(
          `Bind ${queueName} on ${exchange} for ${action} with filter ${JSON.stringify(filterWithMatch)}.`,
        );
        await channel.bindQueue(queueName, exchange, '', { action, ...filterWithMatch });
        this._logger.info(
          `Queue ${queueName} bound to exchange ${exchange} for action ${action} with filter ${JSON.stringify(filterWithMatch)}.`,
        );
      }
    } else {
      this._logger.debug(
        `Bind ${queueName} on ${exchange} with filter ${JSON.stringify(filterWithMatch)}.`,
      );
      await channel.bindQueue(queueName, exchange, '', filterWithMatch);
      this._logger.info(
        `Queue ${queueName} bound to exchange ${exchange} with filter ${JSON.stringify(filterWithMatch)}.`,
      );
    }

    const { consumerTag } = await channel.consume(queueName, (msg) => {
      if (msg === null) {
        this._onConsumerCancelled(sub, channel);
        return;
      }
      return this._onMessage(msg, channel);
    });
    const wanted = this._subscriptionRegistry.includes(sub);
    if (this._channel !== channel) {
      // The consumer died with its channel; a resubscribe restores the registration.
      return wanted;
    }
    sub.consumer = { channel, tag: consumerTag };
    if (!wanted) {
      // Unsubscribed while this consumer was being created.
      await this._cancelConsumer(sub);
    }
    return wanted;
  }

  /**
   * Cancels the registration's consumer if it lives on the current channel; one on a
   * replaced channel died with it.
   *
   * @return false if the cancel failed.
   */
  private async _cancelConsumer(sub: SubscriptionRegistration): Promise<boolean> {
    const consumer = sub.consumer;
    sub.consumer = undefined;
    if (!consumer || consumer.channel !== this._channel) {
      return true;
    }
    try {
      await consumer.channel.cancel(consumer.tag);
      return true;
    } catch (error) {
      this._logger.warn(`Failed to cancel RabbitMQ consumer for ${sub.identifier}`, error);
      return false;
    }
  }

  /**
   * The broker cancels a consumer when its queue is deleted (amqplib then delivers
   * null). Subscribe again while the registration is still wanted.
   */
  private _onConsumerCancelled(sub: SubscriptionRegistration, channel: amqplib.Channel) {
    if (sub.consumer?.channel !== channel || this._channel !== channel) {
      return;
    }
    sub.consumer = undefined;
    if (!this._subscriptionRegistry.includes(sub)) {
      return;
    }
    this._logger.warn(`RabbitMQ cancelled the consumer for ${sub.identifier}; subscribing again.`);
    this._performSubscribe(sub).catch((err) => {
      this._logger.error(`Failed to resubscribe ${sub.identifier} after a broker cancel`, err);
    });
  }

  private async _resubscribeAll(): Promise<void> {
    // unsubscribe replaces the registry and subscribe() handles entries added from
    // now on, so work on a snapshot and skip entries removed meanwhile.
    const subs = [...this._subscriptionRegistry];
    if (!this._channel || subs.length === 0) {
      return;
    }
    this._logger.info(`Resubscribing ${subs.length} RabbitMQ subscription(s) after reconnect`);
    for (const sub of subs) {
      // Consumers on the previous channel died with it.
      sub.consumer = undefined;
    }
    for (const sub of subs) {
      if (!this._subscriptionRegistry.includes(sub)) {
        continue;
      }
      try {
        await this._performSubscribe(sub);
      } catch (err) {
        this._logger.error(
          `Failed to resubscribe RabbitMQ queue for ${sub.identifier}; messaging may be broken until reconnect.`,
          err,
        );
        throw err;
      }
    }
  }
}
