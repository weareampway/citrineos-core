// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type {
  CircuitBreakerState,
  IMessage,
  IMessageConfirmation,
  IMessageSender,
  OcppRequest,
  OcppResponse,
  SystemConfig,
} from '@citrineos/base';
import { AbstractMessageSender, CircuitBreaker, MessageState, OcppError } from '@citrineos/base';
import * as amqplib from 'amqplib';
import { instanceToPlain } from 'class-transformer';
import { RabbitMqConnectionHealth, retireRabbitChannel } from './connection-health.js';
import { getSharedChannel } from './shared-connection-pool.js';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';

/**
 * Implementation of a {@link IMessageSender} using RabbitMQ as the underlying transport.
 */
export class RabbitMqSender extends AbstractMessageSender implements IMessageSender {
  /**
   * Constants
   */
  private static readonly QUEUE_PREFIX = 'amqp_queue_';
  private static readonly RECONNECT_DELAY = 5000;

  /**
   * Fields
   */
  protected _channel?: amqplib.Channel;
  private _connectionAttempt?: Promise<amqplib.Channel>;
  private _connectionHealth = new RabbitMqConnectionHealth();
  private _channelRetirement: Promise<void> = Promise.resolve();
  private _abortReconnectController = new AbortController();
  private _stopped = false;
  private _circuitBreaker: CircuitBreaker;
  private _reconnectInterval?: NodeJS.Timeout;

  /**
   * Constructor for the class.
   *
   * @param {SystemConfig} config - The system configuration.
   * @param {Logger<ILogObj>} [logger] - The logger object.
   */
  constructor(config: SystemConfig, logger?: Logger<ILogObj>, circuitBreaker?: CircuitBreaker) {
    super(config, logger);
    this._circuitBreaker = circuitBreaker ?? new CircuitBreaker();
    this._circuitBreaker.onStateChange(this._onCircuitBreakerStateChange.bind(this));
    this._connectWithRetry().catch((error) => {
      this._logger.error('Failed to connect to RabbitMQ', error);
    });
  }

  /**
   * Methods
   */

  /**
   * Sends a request message with an optional payload and returns a promise that resolves to the confirmation message.
   *
   * @param {IMessage<OcppRequest>} message - The message to be sent.
   * @param {OcppRequest | undefined} payload - The optional payload to be sent with the message.
   * @return {Promise<IMessageConfirmation>} A promise that resolves to the confirmation message.
   */
  sendRequest(
    message: IMessage<OcppRequest>,
    payload?: OcppRequest | undefined,
  ): Promise<IMessageConfirmation> {
    return this.send(message, payload, MessageState.Request);
  }

  /**
   * Sends a response message and returns a promise of the message confirmation.
   *
   * @param {IMessage<OcppResponse | OcppError>} message - The message to send.
   * @param {OcppResponse | OcppError} payload - The payload to include in the response.
   * @return {Promise<IMessageConfirmation>} - A promise that resolves to the message confirmation.
   */
  sendResponse(
    message: IMessage<OcppResponse | OcppError>,
    payload?: OcppResponse | OcppError,
  ): Promise<IMessageConfirmation> {
    return this.send(message, payload, MessageState.Response);
  }

  /**
   * Sends a message and returns a promise that resolves to a message confirmation.
   *
   * @param {IMessage<OcppRequest | OcppResponse | OcppError>} message - The message to be sent.
   * @param {OcppRequest | OcppResponse | OcppError} [payload] - The payload to be included in the message.
   * @param {MessageState} [state] - The state of the message.
   * @return {Promise<IMessageConfirmation>} - A promise that resolves to a message confirmation.
   */
  async send(
    message: IMessage<OcppRequest | OcppResponse | OcppError>,
    payload?: OcppRequest | OcppResponse | OcppError,
    state?: MessageState,
  ): Promise<IMessageConfirmation> {
    if (this._circuitBreaker.state === 'CLOSED') {
      return { success: false, payload: 'Circuit breaker is CLOSED. Cannot send message.' };
    }

    if (payload) {
      message.payload = payload;
    }

    if (state) {
      message.state = state;
    }

    if (!message.state) {
      return { success: false, payload: 'Message state must be set' };
    }

    if (!message.payload) {
      return { success: false, payload: 'Message payload must be set' };
    }

    const exchange = this._config.util.messageBroker.amqp?.exchange as string;
    if (!this._channel) {
      throw new Error('RabbitMQ is down: cannot send message.');
    }
    const channel = this._channel;

    this._logger.debug(`Publishing to ${exchange}:`, message);

    const success = channel.publish(
      exchange || '',
      '',
      Buffer.from(JSON.stringify(instanceToPlain(message)), 'utf-8'),
      {
        contentEncoding: 'utf-8',
        contentType: 'application/json',
        headers: {
          origin: message.origin.toString(),
          eventGroup: message.eventGroup.toString(),
          action: message.action.toString(),
          state: message.state.toString(),
          ...message.context,
          tenantId: message.context.tenantId.toString(),
        },
      },
    );
    return { success };
  }

  /**
   * Shuts down the sender by closing the client.
   *
   * @return {Promise<void>} A promise that resolves when the client is closed.
   */
  async shutdown(): Promise<void> {
    this._stopped = true;
    this._abortReconnectController.abort();
    if (this._reconnectInterval) clearInterval(this._reconnectInterval);
    this._reconnectInterval = undefined;
    const channel = this._channel;
    this._channel = undefined;
    this._connectionHealth.noteDisconnected();
    await retireRabbitChannel(channel);
  }

  get disconnectedSince(): Date | null {
    return this._connectionHealth.disconnectedSince;
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
    this._connectionAttempt ??= this._connectWithRetryLoop(abortSignal).finally(() => {
      this._connectionAttempt = undefined;
    });
    return this._connectionAttempt;
  }

  private async _connectWithRetryLoop(abortSignal: AbortSignal): Promise<amqplib.Channel> {
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
          if (this._channel !== channel) {
            return;
          }
          this._logger.error('AMQP channel error', err);
          this._handleDisconnect();
        });
        channel.on('close', () => {
          if (this._channel === channel) {
            this._handleDisconnect();
          }
        });
        this._channel = channel;
        this._connectionHealth.markConnected();
        this._circuitBreaker.triggerSuccess();
        return channel;
      } catch (err) {
        if (this._stopped || abortSignal?.aborted) throw err;
        this._logger.error('RabbitMQ connect failed, triggering circuit breaker failure', err);
        this._connectionHealth.noteDisconnected();
        this._circuitBreaker.triggerFailure((err as Error)?.message);
        await new Promise((res) => setTimeout(res, 1000));
      }
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
          'Circuit breaker CLOSED: shutting down RabbitMQ sender. Reason:',
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
          'Circuit breaker is FAILING. RabbitMQ sender will not send messages until recovery. Reason:',
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
}
