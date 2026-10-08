// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type {
  BootstrapConfig,
  IMessageRouter,
  INetworkConnection,
  SystemConfig,
  WebsocketServerConfig,
} from '@citrineos/base';
import {
  AbstractModuleApi,
  AsDataEndpoint,
  BadRequestError,
  ConfigStoreFactory,
  DEFAULT_TENANT_ID,
  HttpMethod,
  Namespace,
  NotFoundError,
  OCPP1_6_Namespace,
  OCPP2_0_1_Namespace,
  OCPPVersion,
} from '@citrineos/base';
import type {
  ChargingStationKeyQuerystring,
  ConnectionDeleteQuerystring,
  IServerNetworkProfileRepository,
  ISubscriptionRepository,
  ModelKeyQuerystring,
  TenantQueryString,
  WebsocketDeleteQuerystring,
  WebsocketGetQuerystring,
} from '@citrineos/data';
import {
  ChargingStationKeyQuerySchema,
  ConnectionDeleteQuerySchema,
  CreateSubscriptionSchema,
  ModelKeyQuerystringSchema,
  sequelize,
  Subscription,
  TenantQuerySchema,
  WebsocketDeleteQuerySchema,
  WebsocketGetQuerySchema,
  WebsocketRequestSchema,
} from '@citrineos/data';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import type { IAdminApi } from './interface.js';

/**
 * Admin API for the OcppRouter.
 */
export class AdminApi extends AbstractModuleApi<IMessageRouter> implements IAdminApi {
  private _networkConnection: INetworkConnection;
  private _subscriptionRepository: ISubscriptionRepository;
  private _serverNetworkProfileRepository: IServerNetworkProfileRepository;
  private _websocketOperations: Promise<void> = Promise.resolve();

  /**
   * Constructs a new instance of the class.
   *
   * @param {IMessageRouter} ocppRouter - The OcppRouter module.
   * @param {INetworkConnection} networkConnection - The network connection instance.
   * @param {FastifyInstance} server - The Fastify server instance.
   * @param {BootstrapConfig & SystemConfig} config - The configuration instance.
   * @param {Logger<ILogObj>} [logger] - The logger instance.
   * @param {ISubscriptionRepository} [subscriptionRepository] - The subscription repository instance.
   * @param {IServerNetworkProfileRepository} [serverNetworkProfileRepository] - The server network profile repository instance.
   */
  constructor(
    ocppRouter: IMessageRouter,
    networkConnection: INetworkConnection,
    server: FastifyInstance,
    config: BootstrapConfig & SystemConfig,
    logger?: Logger<ILogObj>,
    subscriptionRepository?: ISubscriptionRepository,
    serverNetworkProfileRepository?: IServerNetworkProfileRepository,
  ) {
    super(ocppRouter, server, null, logger);
    this._networkConnection = networkConnection;
    this._subscriptionRepository =
      subscriptionRepository || new sequelize.SequelizeSubscriptionRepository(config, this._logger);
    this._serverNetworkProfileRepository =
      serverNetworkProfileRepository ||
      new sequelize.SequelizeServerNetworkProfileRepository(config, this._logger);
  }

  // N.B.: When adding subscriptions, chargers may be connected to a different instance of Citrine.
  // If this is the case, new subscriptions will not take effect until the charger reconnects.
  /**
   * Creates a {@link Subscription}.
   * Will always create a new entity and return its id.
   *
   * @param {FastifyRequest<{ Body: Subscription }>} request - The request object, containing the body which is parsed as a {@link Subscription}.
   * @return {Promise<number>} The id of the created subscription.
   */
  @AsDataEndpoint(
    OCPP2_0_1_Namespace.Subscription,
    HttpMethod.Post,
    TenantQuerySchema,
    CreateSubscriptionSchema,
  )
  async postSubscription(
    request: FastifyRequest<{ Body: Subscription; Querystring: TenantQueryString }>,
  ): Promise<number> {
    const tenantId = request.query.tenantId;
    request.body.tenantId = tenantId;
    if (
      !request.body.onClose &&
      !request.body.onConnect &&
      !request.body.onMessage &&
      !request.body.sentMessage
    ) {
      throw new BadRequestError(
        'Must specify at least one of onConnect, onClose, onMessage, sentMessage to true.',
      );
    }
    return this._subscriptionRepository
      .create(tenantId, request.body as Subscription)
      .then((subscription) => subscription?.id);
  }

  @AsDataEndpoint(OCPP2_0_1_Namespace.Subscription, HttpMethod.Get, ChargingStationKeyQuerySchema)
  async getSubscriptionsByChargingStation(
    request: FastifyRequest<{ Querystring: ChargingStationKeyQuerystring }>,
  ): Promise<Subscription[]> {
    return this._subscriptionRepository.readAllByStationId(
      request.query.tenantId,
      request.query.stationId,
    );
  }

  @AsDataEndpoint(OCPP2_0_1_Namespace.Subscription, HttpMethod.Delete, ModelKeyQuerystringSchema)
  async deleteSubscriptionById(
    request: FastifyRequest<{ Querystring: ModelKeyQuerystring }>,
  ): Promise<boolean> {
    const tenantId = request.query.tenantId ?? DEFAULT_TENANT_ID;
    return this._subscriptionRepository
      .deleteByKey(tenantId, request.query.id.toString())
      .then(() => true);
  }

  @AsDataEndpoint(Namespace.Websocket, HttpMethod.Get, WebsocketGetQuerySchema)
  async getWebsocketConfigurations(
    request: FastifyRequest<{ Querystring: WebsocketGetQuerystring }>,
  ): Promise<WebsocketServerConfig[] | WebsocketServerConfig> {
    const config = await ConfigStoreFactory.getInstance().fetchConfig();
    if (!config) {
      throw new NotFoundError('No configuration found in storage');
    }

    const websocketServers = config.util.networkConnection.websocketServers;
    if (request.query.id) {
      const websocketConfig = websocketServers.find((ws) => ws.id === request.query.id);
      if (!websocketConfig) {
        throw new NotFoundError(
          `Could not find websocket configuration with id ${request.query.id}`,
        );
      }
      return websocketConfig;
    }

    const tenantId = request.query.tenantId;
    if (tenantId !== undefined && String(tenantId) !== '') {
      return websocketServers.filter((ws) => ws.tenantId === Number(tenantId));
    }
    return websocketServers;
  }

  @AsDataEndpoint(Namespace.Websocket, HttpMethod.Post, undefined, WebsocketRequestSchema)
  async createWebsocketConfiguration(
    request: FastifyRequest<{ Body: WebsocketServerConfig }>,
  ): Promise<WebsocketServerConfig> {
    return this.enqueueWebsocketOperation(async () => {
      const updated = await ConfigStoreFactory.getInstance().updateConfig((config) => {
        const servers = config.util.networkConnection.websocketServers;
        if (servers.some((ws) => ws.id === request.body.id)) {
          throw new BadRequestError(
            `Websocket configuration with id ${request.body.id} already exists.`,
          );
        }
        if (servers.some((ws) => ws.port === request.body.port)) {
          throw new BadRequestError(
            `Websocket configuration with port ${request.body.port} already exists.`,
          );
        }
        servers.push(request.body);
      });
      this.applyWebsocketServers(updated.util.networkConnection.websocketServers);
      return request.body;
    });
  }

  /**
   * Add new websocket servers for the tenant without restarting the service.
   */
  @AsDataEndpoint(Namespace.Websocket, HttpMethod.Put, TenantQuerySchema)
  async addWebsocketConfigurationsForTenant(
    request: FastifyRequest<{ Body: { tenantId: number }; Querystring: TenantQueryString }>,
  ): Promise<WebsocketServerConfig[]> {
    return this.enqueueWebsocketOperation(async () => {
      const updated = await ConfigStoreFactory.getInstance().updateConfig((config) => {
        const servers = config.util.networkConnection.websocketServers;
        if (servers.some((ws) => ws.tenantId === request.body.tenantId)) {
          return;
        }

        const template = servers[0];
        if (!template) {
          throw new BadRequestError(
            'Cannot create new websocket server: no existing server to copy.',
          );
        }

        const maxPort = servers.reduce((max, ws) => Math.max(max, ws.port), 10000);
        const maxWebsocketPort = 10500;
        if (maxPort + 2 > maxWebsocketPort) {
          throw new BadRequestError(
            'Cannot create new websocket server: maximum port 10500 reached.',
          );
        }

        const maxServerId = servers.reduce((max, ws) => Math.max(max, Number(ws.id)), 0);
        servers.push(
          this.buildWebsocketServer(
            request.body.tenantId,
            maxPort + 1,
            maxServerId + 1,
            0,
            true,
            template,
          ),
          this.buildWebsocketServer(
            request.body.tenantId,
            maxPort + 2,
            maxServerId + 2,
            1,
            false,
            template,
          ),
        );
      });

      this.applyWebsocketServers(updated.util.networkConnection.websocketServers);
      const tenantServers = updated.util.networkConnection.websocketServers.filter(
        (ws) => ws.tenantId === request.body.tenantId,
      );
      for (const server of tenantServers) {
        await this._serverNetworkProfileRepository.upsertServerNetworkProfile(
          server,
          updated.maxCallLengthSeconds,
        );
        await this._networkConnection.addWebsocketServer(server);
      }
      return tenantServers;
    });
  }

  @AsDataEndpoint(Namespace.Websocket, HttpMethod.Delete, WebsocketDeleteQuerySchema)
  async deleteWebsocketConfiguration(
    request: FastifyRequest<{ Querystring: WebsocketDeleteQuerystring }>,
  ): Promise<void> {
    return this.enqueueWebsocketOperation(async () => {
      try {
        const updated = await ConfigStoreFactory.getInstance().updateConfig((config) => {
          const servers = config.util.networkConnection.websocketServers;
          const index = servers.findIndex((ws) => ws.id === request.query.id);
          if (index < 0) {
            throw new MissingWebsocketServerError();
          }
          servers.splice(index, 1);
        });
        this.applyWebsocketServers(updated.util.networkConnection.websocketServers);
      } catch (error) {
        if (!(error instanceof MissingWebsocketServerError)) {
          throw error;
        }
      }
      await this._networkConnection.removeWebsocketServer(request.query.id);
    });
  }

  // Forcibly disconnect a websocket connection by station id and tenant id and mark the station as offline
  @AsDataEndpoint(Namespace.Connection, HttpMethod.Delete, ConnectionDeleteQuerySchema)
  async deleteWebsocketConnection(
    request: FastifyRequest<{ Querystring: ConnectionDeleteQuerystring }>,
  ): Promise<void> {
    await this._networkConnection.disconnect(request.query.tenantId, request.query.stationId);
  }

  /**
   * Overrides superclass method to generate the URL path based on the input {@link Namespace}
   * and the module's endpoint prefix configuration.
   *
   * @param {Namespace} input - The input {@link Namespace}.
   * @return {string} - The generated URL path.
   */
  protected _toDataPath(input: OCPP2_0_1_Namespace | OCPP1_6_Namespace | Namespace): string {
    const endpointPrefix = '/ocpprouter';
    return super._toDataPath(input, endpointPrefix);
  }

  private enqueueWebsocketOperation<T>(operation: () => Promise<T>): Promise<T> {
    // Order local config changes with their profile writes and listener activation.
    // Cross-instance config writes remain protected by the ConfigStore version check.
    const run = this._websocketOperations.then(operation);
    this._websocketOperations = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private applyWebsocketServers(servers: WebsocketServerConfig[]): void {
    const live = this._module.config.util.networkConnection.websocketServers;
    live.splice(0, live.length, ...servers);
  }

  private buildWebsocketServer(
    tenantId: number,
    port: number,
    serverId: number,
    securityProfile: number,
    allowUnknownChargingStations: boolean,
    existingServerConfig: WebsocketServerConfig,
  ): WebsocketServerConfig {
    return {
      id: serverId.toString(),
      host: existingServerConfig.host,
      port,
      pingInterval: existingServerConfig.pingInterval,
      protocol: OCPPVersion.OCPP2_0_1,
      securityProfile,
      tenantId,
      allowUnknownChargingStations,
    };
  }
}

class MissingWebsocketServerError extends Error {
  constructor() {
    super('Websocket server is not in the stored config');
    this.name = 'MissingWebsocketServerError';
  }
}
