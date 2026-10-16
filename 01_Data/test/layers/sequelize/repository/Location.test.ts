// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import { Op } from 'sequelize';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ChargingStation } from '../../../../src/layers/sequelize/model/index.js';
import { SequelizeLocationRepository } from '../../../../src/layers/sequelize/repository/Location.js';

function setup() {
  const logger = { warn: vi.fn() };
  const repository = Object.create(SequelizeLocationRepository.prototype, {
    logger: { value: logger },
  }) as SequelizeLocationRepository;
  const find = vi.spyOn(ChargingStation, 'findOne');
  return { repository, find, logger };
}

describe('websocket station ownership', () => {
  afterEach(() => vi.restoreAllMocks());

  it('accepts a station on its own tenant listener with only one query', async () => {
    const { repository, find, logger } = setup();
    find.mockResolvedValueOnce({ id: 'ampway_16' } as ChargingStation);

    await expect(repository.doesChargingStationExistByStationId(5, 'ampway_16')).resolves.toBe(
      true,
    );

    expect(find).toHaveBeenCalledExactlyOnceWith({
      attributes: ['id'],
      where: { id: 'ampway_16', tenantId: 5 },
    });
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('rejects a foreign station and logs its owner and the listener tenant', async () => {
    const { repository, find, logger } = setup();
    find.mockResolvedValueOnce(null).mockResolvedValueOnce({ tenantId: 5 } as ChargingStation);

    await expect(repository.doesChargingStationExistByStationId(9, 'ampway_16')).resolves.toBe(
      false,
    );

    expect(find).toHaveBeenCalledTimes(2);
    expect(find).toHaveBeenNthCalledWith(1, {
      attributes: ['id'],
      where: { id: 'ampway_16', tenantId: 9 },
    });
    expect(find).toHaveBeenNthCalledWith(2, {
      attributes: ['tenantId'],
      where: { id: 'ampway_16', tenantId: { [Op.ne]: 9 } },
    });
    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      'Charging station ampway_16 belongs to tenant 5, but connected through listener for tenant 9',
      { stationId: 'ampway_16', stationTenantId: 5, listenerTenantId: 9 },
    );
  });

  it('rejects an unknown station without misreporting a tenant mismatch', async () => {
    const { repository, find, logger } = setup();
    find.mockResolvedValue(null);

    await expect(repository.doesChargingStationExistByStationId(5, 'unknown')).resolves.toBe(false);

    expect(find).toHaveBeenCalledTimes(2);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it.each([1, 2])(
    'propagates a database failure in query %i instead of allowing a connection',
    async (query) => {
      const { repository, find, logger } = setup();
      if (query === 2) find.mockResolvedValueOnce(null);
      find.mockRejectedValueOnce(new Error('database unavailable'));

      await expect(repository.doesChargingStationExistByStationId(9, 'ampway_16')).rejects.toThrow(
        'database unavailable',
      );

      expect(find).toHaveBeenCalledTimes(query);
      expect(logger.warn).not.toHaveBeenCalled();
    },
  );
});
