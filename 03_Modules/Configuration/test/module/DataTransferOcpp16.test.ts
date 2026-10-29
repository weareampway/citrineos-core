// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { IMessage, OcppRequest } from '@citrineos/base';
import { MessageOrigin, MessageState, OCPP1_6, OCPPVersion } from '@citrineos/base';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import { describe, expect, it, vi } from 'vitest';
import { ConfigurationModule } from '../../src/module/module.js';

describe('ConfigurationModule OCPP 1.6 DataTransfer', () => {
  it('answers an unknown vendor with UnknownVendorId', async () => {
    const sendResponse = vi.fn(async () => ({ success: true }));
    const module = Object.assign(Object.create(ConfigurationModule.prototype), {
      _logger: new Logger<ILogObj>({ type: 'hidden' }),
      _sender: { sendResponse },
    }) as ConfigurationModule;
    const message = {
      origin: MessageOrigin.ChargingStation,
      action: 'DataTransfer',
      state: MessageState.Request,
      protocol: OCPPVersion.OCPP1_6,
      context: { stationId: 'station', tenantId: 1, correlationId: 'D', timestamp: '' },
      payload: { vendorId: 'P40', messageId: 'generalConfiguration' },
    } as unknown as IMessage<OcppRequest>;

    await module.handle(message);

    expect(sendResponse).toHaveBeenCalledWith(message, {
      status: OCPP1_6.DataTransferResponseStatus.UnknownVendorId,
    });
  });
});
