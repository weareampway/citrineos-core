// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { IMessage, OcppRequest } from '@citrineos/base';
import { MessageOrigin, MessageState, OCPPVersion } from '@citrineos/base';
import type { ILogObj } from 'tslog';
import { Logger } from 'tslog';
import { describe, expect, it, vi } from 'vitest';
import { ReportingModule } from '../../src/module/module.js';

describe('ReportingModule OCPP 1.6 SecurityEventNotification', () => {
  it('stores the event and answers with an empty CallResult', async () => {
    const sendResponse = vi.fn(async () => ({ success: true }));
    const createByStationId = vi.fn(async () => ({}));
    const module = Object.assign(Object.create(ReportingModule.prototype), {
      _logger: new Logger<ILogObj>({ type: 'hidden' }),
      _sender: { sendResponse },
      _securityEventRepository: { createByStationId },
    }) as ReportingModule;
    const payload = { type: 'SecurityLogWasCleared', timestamp: '2026-10-28T12:00:00.000Z' };
    const message = {
      origin: MessageOrigin.ChargingStation,
      action: 'SecurityEventNotification',
      state: MessageState.Request,
      protocol: OCPPVersion.OCPP1_6,
      context: { stationId: 'station', tenantId: 1, correlationId: 'S', timestamp: '' },
      payload,
    } as unknown as IMessage<OcppRequest>;

    await module.handle(message);

    expect(createByStationId).toHaveBeenCalledWith(1, payload, 'station');
    expect(sendResponse).toHaveBeenCalledWith(message, {});
  });
});
