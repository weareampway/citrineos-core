// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { QueryInterface } from 'sequelize';

const deleteOrphanStationData = async (sequelize: QueryInterface['sequelize']) => {
  const orphanCondition = `NOT EXISTS (SELECT 1 FROM "ChargingStations" cs WHERE cs.id = t."stationId" AND cs."tenantId" = t."tenantId")`;

  await sequelize.query(`
    DELETE FROM "MeterValues"
    WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition})
       OR "transactionEventId" IN (SELECT id FROM "TransactionEvents" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition}))
       OR "stopTransactionDatabaseId" IN (SELECT id FROM "StopTransactions" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition}))
  `);
  await sequelize.query(
    `DELETE FROM "ChargingNeeds" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition})`,
  );
  await sequelize.query(
    `DELETE FROM "ChargingProfiles" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition})`,
  );
  await sequelize.query(
    `DELETE FROM "TransactionEvents" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition})`,
  );
  await sequelize.query(
    `DELETE FROM "StartTransactions" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition})`,
  );
  await sequelize.query(
    `DELETE FROM "StopTransactions" WHERE "transactionDatabaseId" IN (SELECT id FROM "Transactions" t WHERE ${orphanCondition})`,
  );
  await sequelize.query(`DELETE FROM "Transactions" t WHERE ${orphanCondition}`);

  await sequelize.query(
    `DELETE FROM "Connectors" WHERE "stationId" NOT IN (SELECT id FROM "ChargingStations")`,
  );
  await sequelize.query(
    `DELETE FROM "Evses" WHERE "stationId" NOT IN (SELECT id FROM "ChargingStations")`,
  );
  await sequelize.query(
    `DELETE FROM "ChargingStationSequences" WHERE "stationId" NOT IN (SELECT id FROM "ChargingStations")`,
  );
  await sequelize.query(
    `DELETE FROM "VariableAttributes" WHERE "stationId" IS NOT NULL AND "stationId" NOT IN (SELECT id FROM "ChargingStations")`,
  );
  await sequelize.query(
    `DELETE FROM "StatusNotifications" WHERE "stationId" IS NOT NULL AND "stationId" NOT IN (SELECT id FROM "ChargingStations")`,
  );
  await sequelize.query(
    `DELETE FROM "LatestStatusNotifications" WHERE "stationId" IS NOT NULL AND "stationId" NOT IN (SELECT id FROM "ChargingStations")`,
  );
  await sequelize.query(
    `DELETE FROM "ChargingStationNetworkProfiles" WHERE NOT EXISTS (SELECT 1 FROM "ChargingStations" cs WHERE cs.id = "ChargingStationNetworkProfiles"."stationId" AND cs."tenantId" = "ChargingStationNetworkProfiles"."tenantId")`,
  );
  await sequelize.query(
    `DELETE FROM "Boots" WHERE NOT EXISTS (SELECT 1 FROM "ChargingStations" cs WHERE cs.id = "Boots".id AND cs."tenantId" = "Boots"."tenantId")`,
  );
};

export default {
  up: async (queryInterface: QueryInterface): Promise<void> => {
    await deleteOrphanStationData(queryInterface.sequelize);
  },
  down: async () => {},
};
