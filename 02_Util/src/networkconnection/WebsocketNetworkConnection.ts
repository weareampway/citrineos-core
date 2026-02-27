// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
/* eslint-disable */
import type {
  IAuthenticator,
  ICache,
  IMessageRouter,
  INetworkConnection,
  IWebsocketConnection,
  OCPPVersionType,
  SystemConfig,
  WebsocketServerConfig,
} from '@citrineos/base';
import {
  CacheNamespace,
  createIdentifier,
  getStationIdFromIdentifier,
  getTenantIdFromIdentifier,
} from '@citrineos/base';
import fs from 'fs';
import * as http from 'http';
import * as https from 'https';
import { Duplex } from 'stream';
import type { SecureContextOptions } from 'tls';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import type { ErrorEvent, MessageEvent } from 'ws';
import { WebSocket, WebSocketServer } from 'ws';
import type { IUpgradeError } from './authenticator/errors/IUpgradeError.js';

type IWebsocketConnectionWithOwner = IWebsocketConnection & { instanceId?: string };

export class WebsocketNetworkConnection implements INetworkConnection {
  protected _cache: ICache;
  protected _config: SystemConfig;
  protected _logger: Logger<ILogObj>;
  private _identifierConnections: Map<string, WebSocket> = new Map();
  // websocketServers id as key and http server as value
  private _httpServersMap: Map<string, http.Server | https.Server>;
  private _authenticator: IAuthenticator;
  private _router: IMessageRouter;
  private readonly _instanceId: string;

  constructor(
    config: SystemConfig,
    cache: ICache,
    authenticator: IAuthenticator,
    router: IMessageRouter,
    logger?: Logger<ILogObj>,
  ) {
    this._cache = cache;
    this._config = config;
    this._logger = logger
      ? logger.getSubLogger({ name: this.constructor.name })
      : new Logger<ILogObj>({ name: this.constructor.name });
    this._authenticator = authenticator;
    router.networkHook = this.sendMessage.bind(this);
    this._router = router;
    this._instanceId = process.env.HOSTNAME || `pid-${process.pid}`;

    this._httpServersMap = new Map<string, http.Server | https.Server>();
    this._config.util.networkConnection.websocketServers.forEach(async (websocketServerConfig) => {
      const _httpServer = await this._createAndStartWebsocketServer(websocketServerConfig);
      this._httpServersMap.set(websocketServerConfig.id, _httpServer);
    });
  }

  /**
   * Send a message to the charging station specified by the identifier.
   *
   * @param {string} identifier - The identifier of the client.
   * @param {string} message - The message to send.
   * @return {void} rejects the promise if message fails to send, otherwise returns void.
   */
  sendMessage(identifier: string, message: string): Promise<void> {
    return new Promise<void>(async (resolve, reject) => {
      try {
        const websocketConnection = this._identifierConnections.get(identifier);
        if (websocketConnection && websocketConnection.readyState === WebSocket.OPEN) {
          websocketConnection.send(message, (error) => {
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          });
          return;
        }

        const clientConnection = await this._cache.get(identifier, CacheNamespace.Connections);
        const errorMsg = clientConnection
          ? 'Websocket connection is not ready - ' + identifier
          : 'Cannot identify client connection for ' + identifier;
        this._logger.error(errorMsg);
        websocketConnection?.close(1011, errorMsg);
        reject(new Error(errorMsg));
      } catch (error) {
        reject(error);
      }
    });
  }

  bindNetworkHook(): (identifier: string, message: string) => Promise<void> {
    return (identifier: string, message: string) => this.sendMessage(identifier, message);
  }

  async disconnect(tenantId: number, stationId: string): Promise<boolean> {
    const identifier = createIdentifier(tenantId, stationId);

    const websocketConnection = this._identifierConnections.get(identifier);

    if (!websocketConnection) {
      this._logger.warn(
        `No websocket connection found for tenantId ${tenantId} and stationId ${stationId}, will still deregister from router.`,
      );
    }
    websocketConnection?.close(1000, 'Disconnected by admin request');
    const deregistered = await this._router?.deregisterConnection(tenantId, stationId);

    return !!websocketConnection && deregistered;
  }

  async shutdown(): Promise<void> {
    this._httpServersMap.forEach((server) => server.close());
  }

  /**
   * Updates certificates for a specific server with the provided TLS key, certificate chain, and optional
   * root CA.
   *
   * @param {string} serverId - The ID of the server to update.
   * @param {string} tlsKey - The TLS key to set.
   * @param {string} tlsCertificateChain - The TLS certificate chain to set.
   * @param {string} [rootCA] - The root CA to set (optional).
   * @return {void} void
   */
  updateTlsCertificates(
    serverId: string,
    tlsKey: string,
    tlsCertificateChain: string,
    rootCA?: string,
  ): void {
    let httpsServer = this._httpServersMap.get(serverId);

    if (httpsServer && httpsServer instanceof https.Server) {
      const secureContextOptions: SecureContextOptions = {
        key: tlsKey,
        cert: tlsCertificateChain,
      };
      if (rootCA) {
        secureContextOptions.ca = rootCA;
      }
      httpsServer.setSecureContext(secureContextOptions);
      this._logger.info(`Updated TLS certificates in SecureContextOptions for server ${serverId}`);
    } else {
      throw new TypeError(`Server ${serverId} is not a https server.`);
    }
  }

  /**
   * Dynamically adds a new websocket server at runtime and starts it.
   *
   * @param {WebsocketServerConfig} websocketServerConfig
   * @returns {Promise<void>}
   */
  async addWebsocketServer(websocketServerConfig: WebsocketServerConfig): Promise<void> {
    const httpServer = await this._createAndStartWebsocketServer(websocketServerConfig);
    this._httpServersMap.set(websocketServerConfig.id, httpServer);
  }

  private _onHttpRequest(req: http.IncomingMessage, res: http.ServerResponse) {
    if (req.method === 'GET' && req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'healthy' }));
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          message: `Route ${req.method}:${req.url} not found`,
          error: 'Not Found',
          statusCode: 404,
        }),
      );
    }
  }

  /**
   * Method to validate websocket upgrade requests and pass them to the socket server.
   *
   * @param {IncomingMessage} req - The request object.
   * @param {Duplex} socket - Websocket duplex stream.
   * @param {Buffer} head - Websocket buffer.
   * @param {WebSocketServer} wss - Websocket server.
   * @param {WebsocketServerConfig} websocketServerConfig - websocket server config.
   */
  private async _upgradeRequest(
    req: http.IncomingMessage,
    socket: Duplex,
    head: Buffer,
    wss: WebSocketServer,
    websocketServerConfig: WebsocketServerConfig,
  ) {
    // Failed mTLS and TLS requests are rejected by the server before getting this far
    this._logger.debug('On upgrade request', req.method, req.url, req.headers);

    try {
      const { identifier } = await this._authenticator.authenticate(
        req,
        websocketServerConfig.tenantId,
        {
          securityProfile: websocketServerConfig.securityProfile,
          allowUnknownChargingStations: websocketServerConfig.allowUnknownChargingStations,
        },
      );

      this._logger.debug('Successfully registered websocket client', identifier);

      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req);
      });
    } catch (error: any) {
      /**
       * See {@link IUpgradeError.terminateConnection}
       **/
      error?.terminateConnection?.(socket) || this._terminateConnectionInternalError(socket);
      this._logger.warn(error);
    }
  }

  /**
   * Utility function to reject websocket upgrade requests with 500 status code.
   * @param socket - Websocket duplex stream.
   */
  private _terminateConnectionInternalError(socket: Duplex) {
    socket.write('HTTP/1.1 500 Internal Server Error\r\n');
    socket.write('\r\n');
    socket.end();
    socket.destroy();
  }

  /**
   * Internal method to handle new client connection and ensures supported protocols are used.
   *
   * @param {Set<string>} protocols - The set of protocols to handle.
   * @param {IncomingMessage} _req - The request object.
   * @param {string} wsServerProtocol - The websocket server protocol.
   * @return {boolean|string} - Returns the protocol version if successful, otherwise false.
   */
  private _handleProtocols(
    protocols: Set<string>,
    _req: http.IncomingMessage,
    wsServerProtocol: OCPPVersionType,
  ) {
    // Only supports configured protocol version
    if (protocols.has(wsServerProtocol)) {
      return wsServerProtocol;
    }
    this._logger.error(
      `Protocol mismatch. Charger supports: [${[...protocols].join(', ')}], but server expects: '${wsServerProtocol}'.`,
    );
    // Reject the client trying to connect
    return false;
  }

  /**
   * Internal method to handle the connection event when a WebSocket connection is established.
   * This happens after successful protocol exchange with client.
   *
   * @param {WebSocket} ws - The WebSocket object representing the connection.
   * @param {WebsocketServerConfig} websocketServerConfig - The websocket server configuration.
   * @param {number} pingInterval - The ping interval in seconds.
   * @param {IncomingMessage} req - The request object associated with the connection.
   * @return {void}
   */
  private async _onConnection(
    ws: WebSocket,
    websocketServerConfig: WebsocketServerConfig,
    pingInterval: number,
    req: http.IncomingMessage,
  ): Promise<void> {
    if (!ws.protocol) {
      this._logger.debug('Websocket connection without protocol');
      return;
    } else {
      // Pause the WebSocket event emitter until broker is established
      ws.pause();

      const stationId = this._getClientIdFromUrl(req.url as string);
      const tenantId = websocketServerConfig.tenantId;
      const identifier = createIdentifier(tenantId, stationId);

      const existingConnection = this._identifierConnections.get(identifier);
      if (existingConnection && existingConnection !== ws) {
        this._logger.warn('Replacing existing websocket connection for identifier', identifier);
        // Ensure stale sockets don't continue to emit messages for the same identifier.
        existingConnection.close(1000, 'Superseded by a new websocket connection');
      }

      this._identifierConnections.set(identifier, ws);

      try {
        // Get IP address of client
        const ip =
          req.headers['x-forwarded-for']?.toString().split(',')[0].trim() ||
          req.socket.remoteAddress ||
          'N/A';
        const port = req.socket.remotePort as number;
        this._logger.info('Client websocket connected', identifier, ip, port, ws.protocol);

        // Register client
        const websocketConnection = {
          id: websocketServerConfig.id,
          protocol: ws.protocol,
          instanceId: this._instanceId,
        };
        let registered = await this._cache.set(
          identifier,
          JSON.stringify(websocketConnection),
          CacheNamespace.Connections,
        );
        registered =
          registered && (await this._router.registerConnection(tenantId, stationId, ws.protocol));
        if (!registered) {
          this._logger.fatal('Failed to register websocket client', identifier);
          throw new Error('Failed to register websocket client');
        }

        this._logger.info('Successfully connected new charging station.', identifier);

        // Register all websocket events
        this._registerWebsocketEvents(identifier, ws, pingInterval);

        // Resume the WebSocket event emitter after events have been subscribed to
        ws.resume();
      } catch (error) {
        this._logger.fatal('Failed to subscribe to message broker for ', identifier);
        ws.close(1011, 'Failed to subscribe to message broker for ' + identifier);
      }
    }
  }

  /**
   * Internal method to register event listeners for the WebSocket connection.
   *
   * @param {string} identifier - The unique identifier of the connection, i.e. the combination of tenantId and stationId.
   * @param {WebSocket} ws - The WebSocket object representing the connection.
   * @param {number} pingInterval - The ping interval in seconds.
   * @return {void} This function does not return anything.
   */
  private _registerWebsocketEvents(identifier: string, ws: WebSocket, pingInterval: number): void {
    ws.onerror = (event: ErrorEvent) => {
      this._logger.error(
        'Connection error encountered for',
        identifier,
        event.error,
        event.message,
        event.type,
      );
      ws.close(1011, event.message);
    };
    ws.onmessage = (event: MessageEvent) => {
      this._onMessage(identifier, event.data.toString(), ws.protocol as OCPPVersionType);
    };

    ws.once('close', () => {
      void this._cleanupClosedConnection(identifier, ws);
    });

    ws.on('ping', async (message) => {
      this._logger.debug(`Ping received for ${identifier} with message ${JSON.stringify(message)}`);
      ws.pong(message);
    });

    ws.on('pong', async () => {
      this._logger.debug('Pong received for', identifier);
      const clientConnection: string | null = await this._cache.get(
        identifier,
        CacheNamespace.Connections,
      );

      if (clientConnection) {
        await this._cache.set(
          identifier,
          clientConnection,
          CacheNamespace.Connections,
          pingInterval * 2,
        );
        this._ping(identifier, ws, pingInterval);
      } else {
        this._logger.debug('Pong received for', identifier, 'but client is not alive');
        ws.close(1011, 'Client is not alive');
      }
    });

    this._ping(identifier, ws, pingInterval);
  }

  /**
   * Internal method to handle the incoming message from the websocket client.
   *
   * @param {string} identifier - The client identifier.
   * @param {string} message - The incoming message from the client.
   * @param {OCPPVersionType} protocol - The OCPP protocol version of the client, 'ocpp1.6' or 'ocpp2.0.1'.
   * @return {void} This function does not return anything.
   */
  private _onMessage(identifier: string, message: string, protocol: OCPPVersionType): void {
    this._router.onMessage(identifier, message, new Date(), protocol);
  }

  /**
   * Internal method to handle the error event for the WebSocket server.
   *
   * @param {WebSocketServer} wss - The WebSocket server instance.
   * @param {Error} error - The error object.
   * @return {void} This function does not return anything.
   */
  private _onError(wss: WebSocketServer, error: Error): void {
    this._logger.error(error);
    // TODO: Try to recover the Websocket server
  }

  /**
   * Internal method to handle the event when the WebSocketServer is closed.
   *
   * @param {WebSocketServer} wss - The WebSocketServer instance.
   * @return {void} This function does not return anything.
   */
  private _onClose(wss: WebSocketServer): void {
    this._logger.debug('Websocket Server closed');
    // TODO: Try to recover the Websocket server
  }

  /**
   * Internal method to execute a ping operation on a WebSocket connection after a delay of 60 seconds.
   *
   * @param {string} identifier - The identifier of the client connection.
   * @param {WebSocket} ws - The WebSocket connection to ping.
   * @param {number} pingInterval - The ping interval in milliseconds.
   * @return {void} This function does not return anything.
   */
  private async _ping(identifier: string, ws: WebSocket, pingInterval: number): Promise<void> {
    setTimeout(async () => {
      const clientConnection: string | null = await this._cache.get(
        identifier,
        CacheNamespace.Connections,
      );
      if (clientConnection) {
        this._logger.debug('Pinging client', identifier);
        // Set connection expiration and send ping to client
        await this._cache.set(
          identifier,
          clientConnection,
          CacheNamespace.Connections,
          pingInterval * 2,
        );
        ws.ping();
      } else {
        ws.close(1011, 'Client is not alive');
      }
    }, pingInterval * 1000);
  }

  private async _cleanupClosedConnection(identifier: string, ws: WebSocket): Promise<void> {
    const activeConnection = this._identifierConnections.get(identifier);
    if (activeConnection && activeConnection !== ws) {
      this._logger.debug('Ignoring close event for stale websocket connection', identifier);
      return;
    }

    this._identifierConnections.delete(identifier);
    try {
      const cachedConnection = await this._cache.get<string>(
        identifier,
        CacheNamespace.Connections,
      );
      let connectionInfo: IWebsocketConnectionWithOwner | null = null;
      if (cachedConnection) {
        try {
          connectionInfo = JSON.parse(cachedConnection) as IWebsocketConnectionWithOwner;
        } catch {
          // Keep previous behavior if cache payload is malformed.
        }
      }

      const ownsSharedConnection =
        !connectionInfo?.instanceId || connectionInfo.instanceId === this._instanceId;
      if (ownsSharedConnection) {
        await this._cache.remove(identifier, CacheNamespace.Connections);
        await this._router.deregisterConnection(
          getTenantIdFromIdentifier(identifier),
          getStationIdFromIdentifier(identifier),
        );
      } else {
        this._logger.debug(
          'Skipping shared connection cleanup from non-owning instance',
          identifier,
          connectionInfo?.instanceId ?? 'unknown',
        );
        await this._router.deregisterLocalConnection(
          getTenantIdFromIdentifier(identifier),
          getStationIdFromIdentifier(identifier),
        );
      }

      this._logger.info('Connection closed for', identifier);
    } catch (error) {
      this._logger.error('Failed during websocket close cleanup for', identifier, error);
    }
  }
  /**
   *
   * @param url Http upgrade request url used by charger
   * @returns Charger identifier
   */
  private _getClientIdFromUrl(url: string): string {
    return url.split('/').pop() as string;
  }

  private _generateServerOptions(config: WebsocketServerConfig): https.ServerOptions {
    const serverOptions: https.ServerOptions = {
      key: fs.readFileSync(config.tlsKeyFilePath as string),
      cert: fs.readFileSync(config.tlsCertificateChainFilePath as string),
    };

    if (config.rootCACertificateFilePath) {
      serverOptions.ca = fs.readFileSync(config.rootCACertificateFilePath as string);
    }

    if (config.securityProfile > 2) {
      serverOptions.requestCert = true;
      serverOptions.rejectUnauthorized = true;
    } else {
      serverOptions.rejectUnauthorized = false;
    }

    return serverOptions;
  }

  private _createAndStartWebsocketServer(
    wsConfig: WebsocketServerConfig,
  ): Promise<http.Server | https.Server> {
    return new Promise((resolve) => {
      let httpServer: http.Server | https.Server;
      switch (wsConfig.securityProfile) {
        case 3: // mTLS
        case 2: // TLS
          httpServer = https.createServer(
            this._generateServerOptions(wsConfig),
            this._onHttpRequest.bind(this),
          );
          break;
        case 1:
        case 0:
        default:
          httpServer = http.createServer(this._onHttpRequest.bind(this));
          break;
      }

      const wss = new WebSocketServer({
        noServer: true,
        handleProtocols: (protocols, req) =>
          this._handleProtocols(protocols, req, wsConfig.protocol as OCPPVersionType),
        clientTracking: false,
      });

      wss.on('connection', (ws, req) =>
        this._onConnection(ws, wsConfig, wsConfig.pingInterval, req),
      );
      wss.on('error', (server: any, error: any) => this._onError(server, error));
      wss.on('close', (server: any) => this._onClose(server));

      httpServer.on('upgrade', (req, socket, head) =>
        this._upgradeRequest(req, socket, head, wss, wsConfig),
      );
      httpServer.on('error', (error) => wss.emit('error', error));
      httpServer.on('close', () => wss.emit('close'));

      const protocol = wsConfig.securityProfile > 1 ? 'wss' : 'ws';
      httpServer.listen(wsConfig.port, wsConfig.host, () => {
        this._logger.info(
          `WebsocketServer running on ${protocol}://${wsConfig.host}:${wsConfig.port}/`,
        );
        resolve(httpServer);
      });
    });
  }
}
