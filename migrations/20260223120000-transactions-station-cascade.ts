// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { QueryInterface } from 'sequelize';

const constraintExists = async (
  sequelize: QueryInterface['sequelize'],
  table: string,
  constraint: string,
): Promise<boolean> => {
  const [results] = await sequelize.query(
    `SELECT 1 FROM information_schema.table_constraints 
         WHERE table_schema = 'public' AND table_name = '${table}' AND constraint_name = '${constraint}'`,
  );
  return (results as unknown[]).length > 0;
};

const dropAndAddCascade = async (
  sequelize: QueryInterface['sequelize'],
  table: string,
  constraint: string,
  columns: string[],
  refTable: string,
  refColumns: string[],
  alsoDrop?: string[],
) => {
  await sequelize.query(`ALTER TABLE "${table}" DROP CONSTRAINT IF EXISTS "${constraint}"`);
  for (const c of alsoDrop ?? []) {
    await sequelize.query(`ALTER TABLE "${table}" DROP CONSTRAINT IF EXISTS "${c}"`);
  }
  const fkCols = columns.map((c) => `"${c}"`).join(', ');
  const refCols = refColumns.map((c) => `"${c}"`).join(', ');
  await sequelize.query(
    `ALTER TABLE "${table}" ADD CONSTRAINT "${constraint}" ` +
      `FOREIGN KEY (${fkCols}) REFERENCES "${refTable}" (${refCols}) ` +
      `ON UPDATE CASCADE ON DELETE CASCADE`,
  );
};

export default {
  up: async (queryInterface: QueryInterface): Promise<void> => {
    const sequelize = queryInterface.sequelize;

    if (
      !(await constraintExists(sequelize, 'ChargingStations', 'ChargingStations_id_tenantId_key'))
    ) {
      await sequelize.query(
        `ALTER TABLE "ChargingStations" ADD CONSTRAINT "ChargingStations_id_tenantId_key" ` +
          `UNIQUE (id, "tenantId")`,
      );
    }

    await dropAndAddCascade(
      sequelize,
      'Transactions',
      'Transactions_stationId_tenantId_fkey',
      ['stationId', 'tenantId'],
      'ChargingStations',
      ['id', 'tenantId'],
      ['Transactions_stationId_fkey'],
    );
    await dropAndAddCascade(
      sequelize,
      'Evses',
      'Evses_stationId_fkey',
      ['stationId'],
      'ChargingStations',
      ['id'],
    );
    await dropAndAddCascade(
      sequelize,
      'Connectors',
      'Connectors_stationId_fkey',
      ['stationId'],
      'ChargingStations',
      ['id'],
    );
    await dropAndAddCascade(
      sequelize,
      'ChargingStationSequences',
      'ChargingStationSequences_stationId_fkey',
      ['stationId'],
      'ChargingStations',
      ['id'],
    );
    await dropAndAddCascade(
      sequelize,
      'VariableAttributes',
      'VariableAttributes_stationId_fkey',
      ['stationId'],
      'ChargingStations',
      ['id'],
    );
    await dropAndAddCascade(
      sequelize,
      'StatusNotifications',
      'StatusNotifications_stationId_fkey',
      ['stationId'],
      'ChargingStations',
      ['id'],
    );
    await dropAndAddCascade(
      sequelize,
      'LatestStatusNotifications',
      'LatestStatusNotifications_stationId_fkey',
      ['stationId'],
      'ChargingStations',
      ['id'],
    );

    await dropAndAddCascade(
      sequelize,
      'ChargingStationNetworkProfiles',
      'ChargingStationNetworkProfiles_stationId_tenantId_fkey',
      ['stationId', 'tenantId'],
      'ChargingStations',
      ['id', 'tenantId'],
      ['ChargingStationNetworkProfiles_stationId_fkey'],
    );

    await sequelize.query(`ALTER TABLE "Boots" DROP CONSTRAINT IF EXISTS "Boots_id_tenantId_fkey"`);
    await sequelize.query(
      `ALTER TABLE "Boots" ADD CONSTRAINT "Boots_id_tenantId_fkey" ` +
        `FOREIGN KEY (id, "tenantId") REFERENCES "ChargingStations" (id, "tenantId") ` +
        `ON UPDATE CASCADE ON DELETE CASCADE`,
    );
  },

  down: async (queryInterface: QueryInterface): Promise<void> => {
    const sequelize = queryInterface.sequelize;

    await sequelize.query(`ALTER TABLE "Boots" DROP CONSTRAINT IF EXISTS "Boots_id_tenantId_fkey"`);

    await sequelize.query(
      `ALTER TABLE "ChargingStationNetworkProfiles" DROP CONSTRAINT IF EXISTS "ChargingStationNetworkProfiles_stationId_tenantId_fkey"`,
    );
    await sequelize.query(
      `ALTER TABLE "ChargingStationNetworkProfiles" ADD CONSTRAINT "ChargingStationNetworkProfiles_stationId_fkey" ` +
        `FOREIGN KEY ("stationId") REFERENCES "ChargingStations" (id) ON UPDATE CASCADE ON DELETE CASCADE`,
    );

    const restoreConstraint = async (
      table: string,
      constraint: string,
      column: string,
      refTable: string,
      refColumn: string,
      onDelete: string,
    ) => {
      await sequelize.query(`ALTER TABLE "${table}" DROP CONSTRAINT IF EXISTS "${constraint}"`);
      await sequelize.query(
        `ALTER TABLE "${table}" ADD CONSTRAINT "${constraint}" ` +
          `FOREIGN KEY ("${column}") REFERENCES "${refTable}" ("${refColumn}") ON UPDATE CASCADE ON DELETE ${onDelete}`,
      );
    };

    await sequelize.query(
      `ALTER TABLE "Transactions" DROP CONSTRAINT IF EXISTS "Transactions_stationId_tenantId_fkey"`,
    );
    await sequelize.query(
      `ALTER TABLE "Transactions" ADD CONSTRAINT "Transactions_stationId_fkey" ` +
        `FOREIGN KEY ("stationId") REFERENCES "ChargingStations" (id) ON UPDATE CASCADE`,
    );

    await restoreConstraint(
      'Evses',
      'Evses_stationId_fkey',
      'stationId',
      'ChargingStations',
      'id',
      'SET NULL',
    );
    await restoreConstraint(
      'Connectors',
      'Connectors_stationId_fkey',
      'stationId',
      'ChargingStations',
      'id',
      'SET NULL',
    );
    await restoreConstraint(
      'ChargingStationSequences',
      'ChargingStationSequences_stationId_fkey',
      'stationId',
      'ChargingStations',
      'id',
      'RESTRICT',
    );
    await restoreConstraint(
      'VariableAttributes',
      'VariableAttributes_stationId_fkey',
      'stationId',
      'ChargingStations',
      'id',
      'RESTRICT',
    );
    await restoreConstraint(
      'StatusNotifications',
      'StatusNotifications_stationId_fkey',
      'stationId',
      'ChargingStations',
      'id',
      'SET NULL',
    );
    await restoreConstraint(
      'LatestStatusNotifications',
      'LatestStatusNotifications_stationId_fkey',
      'stationId',
      'ChargingStations',
      'id',
      'SET NULL',
    );

    await sequelize.query(
      `ALTER TABLE "ChargingStations" DROP CONSTRAINT IF EXISTS "ChargingStations_id_tenantId_key"`,
    );
  },
};
