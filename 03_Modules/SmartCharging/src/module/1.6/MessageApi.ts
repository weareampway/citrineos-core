// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import type { ISmartChargingModuleApi } from '../interface.js';
import { SmartChargingModule } from '../module.js';
import type { CallAction, IMessageConfirmation } from '@citrineos/base';
import {
  AbstractModuleApi,
  AsMessageEndpoint,
  DEFAULT_TENANT_ID,
  OCPP1_6,
  OCPP1_6_CallAction,
  OCPPVersion,
} from '@citrineos/base';
import type { FastifyInstance } from 'fastify';

export class SmartChargingOcpp16Api
  extends AbstractModuleApi<SmartChargingModule>
  implements ISmartChargingModuleApi
{
  constructor(
    smartChargingModule: SmartChargingModule,
    server: FastifyInstance,
    logger?: Logger<ILogObj>,
  ) {
    super(smartChargingModule, server, OCPPVersion.OCPP1_6, logger);
  }

  @AsMessageEndpoint(
    OCPP1_6_CallAction.ClearChargingProfile,
    OCPP1_6.ClearChargingProfileRequestSchema,
  )
  async clearChargingProfile(
    identifier: string[],
    request: OCPP1_6.ClearChargingProfileRequest,
    callbackUrl?: string,
    tenantId: number = DEFAULT_TENANT_ID,
  ): Promise<IMessageConfirmation[]> {
    return Promise.all(
      identifier.map((id) =>
        this._module.sendCall(
          id,
          tenantId,
          OCPPVersion.OCPP1_6,
          OCPP1_6_CallAction.ClearChargingProfile,
          request,
          callbackUrl,
        ),
      ),
    );
  }

  @AsMessageEndpoint(
    OCPP1_6_CallAction.GetCompositeSchedule,
    OCPP1_6.GetCompositeScheduleRequestSchema,
  )
  async getCompositeSchedule(
    identifier: string[],
    request: OCPP1_6.GetCompositeScheduleRequest,
    callbackUrl?: string,
    tenantId: number = DEFAULT_TENANT_ID,
  ): Promise<IMessageConfirmation[]> {
    return Promise.all(
      identifier.map((id) =>
        this._module.sendCall(
          id,
          tenantId,
          OCPPVersion.OCPP1_6,
          OCPP1_6_CallAction.GetCompositeSchedule,
          request,
          callbackUrl,
        ),
      ),
    );
  }

  @AsMessageEndpoint(OCPP1_6_CallAction.SetChargingProfile, OCPP1_6.SetChargingProfileRequestSchema)
  async setChargingProfile(
    identifier: string[],
    request: OCPP1_6.SetChargingProfileRequest,
    callbackUrl?: string,
    tenantId: number = DEFAULT_TENANT_ID,
  ): Promise<IMessageConfirmation[]> {
    return Promise.all(
      identifier.map((id) =>
        this._module.sendCall(
          id,
          tenantId,
          OCPPVersion.OCPP1_6,
          OCPP1_6_CallAction.SetChargingProfile,
          request,
          callbackUrl,
        ),
      ),
    );
  }

  protected _toMessagePath(input: CallAction): string {
    const endpointPrefix = this._module.config.modules.smartcharging?.endpointPrefix;
    return super._toMessagePath(input, endpointPrefix);
  }
}
