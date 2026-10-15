// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ChargingStation } from '../../../../src/layers/sequelize/model/index.js';
import { SequelizeLocationRepository } from '../../../../src/layers/sequelize/repository/Location.js';

describe('websocket station ownership', () => {
  afterEach(() => vi.restoreAllMocks());

  it('rejects a known station on a different tenant listener', async () => {
    const find = vi.spyOn(ChargingStation, 'findOne').mockImplementation(async (options) => {
      const where = options?.where as { id: string; tenantId?: number };
      // Reproduce the production mismatch: this ID exists, but only for tenant 5.
      return where.id === 'ampway_16' && (where.tenantId === undefined || where.tenantId === 5)
        ? ({ id: 'ampway_16', tenantId: 5 } as ChargingStation)
        : null;
    });
    const repository = Object.create(
      SequelizeLocationRepository.prototype,
    ) as SequelizeLocationRepository;
    repository.chargingStation = {
      existsByKey: vi.fn(async (_tenantId: number, id: string) => id === 'ampway_16'),
    } as unknown as typeof repository.chargingStation;

    await expect(repository.doesChargingStationExistByStationId(9, 'ampway_16')).resolves.toBe(
      false,
    );
    await expect(repository.doesChargingStationExistByStationId(5, 'ampway_16')).resolves.toBe(
      true,
    );
    await expect(repository.doesChargingStationExistByStationId(5, 'unknown')).resolves.toBe(false);
    expect(find).toHaveBeenCalledWith({
      attributes: ['id'],
      where: { id: 'ampway_16', tenantId: 9 },
    });
  });

  it('propagates a database failure instead of allowing a connection', async () => {
    vi.spyOn(ChargingStation, 'findOne').mockRejectedValue(new Error('database unavailable'));
    const repository = Object.create(
      SequelizeLocationRepository.prototype,
    ) as SequelizeLocationRepository;
    await expect(repository.doesChargingStationExistByStationId(9, 'ampway_16')).rejects.toThrow(
      'database unavailable',
    );
  });
});
