// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { QueryInterface } from 'sequelize';
import { QueryTypes } from 'sequelize';

interface FkConstraint {
  table_name: string;
  constraint_name: string;
  column_name: string;
  referenced_table: string;
  referenced_column: string;
}

const getTenantFkConstraints = async (
  sequelize: QueryInterface['sequelize'],
): Promise<FkConstraint[]> => {
  const results = await sequelize.query(
    `SELECT
      tc.table_name,
      tc.constraint_name,
      kcu.column_name,
      ccu.table_name AS referenced_table,
      ccu.column_name AS referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = 'public'
      AND ccu.table_name = 'Tenants'
      AND ccu.column_name = 'id'`,
    { type: QueryTypes.SELECT },
  );
  const rows = Array.isArray(results) ? results : [results];
  return rows as FkConstraint[];
};

const getTenantPartnerFkConstraints = async (
  sequelize: QueryInterface['sequelize'],
): Promise<FkConstraint[]> => {
  const results = await sequelize.query(
    `SELECT
      tc.table_name,
      tc.constraint_name,
      kcu.column_name,
      ccu.table_name AS referenced_table,
      ccu.column_name AS referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = 'public'
      AND ccu.table_name = 'TenantPartners'
      AND ccu.column_name = 'id'`,
    { type: QueryTypes.SELECT },
  );
  const rows = Array.isArray(results) ? results : [results];
  return rows as FkConstraint[];
};

const alterFkToCascade = async (
  sequelize: QueryInterface['sequelize'],
  table: string,
  constraint: string,
  column: string,
  refTable: string,
  refColumn: string,
) => {
  await sequelize.query(`ALTER TABLE "${table}" DROP CONSTRAINT IF EXISTS "${constraint}"`);
  await sequelize.query(
    `ALTER TABLE "${table}" ADD CONSTRAINT "${constraint}" ` +
      `FOREIGN KEY ("${column}") REFERENCES "${refTable}" ("${refColumn}") ` +
      `ON UPDATE CASCADE ON DELETE CASCADE`,
  );
};

const alterFkToRestrict = async (
  sequelize: QueryInterface['sequelize'],
  table: string,
  constraint: string,
  column: string,
  refTable: string,
  refColumn: string,
) => {
  await sequelize.query(`ALTER TABLE "${table}" DROP CONSTRAINT IF EXISTS "${constraint}"`);
  await sequelize.query(
    `ALTER TABLE "${table}" ADD CONSTRAINT "${constraint}" ` +
      `FOREIGN KEY ("${column}") REFERENCES "${refTable}" ("${refColumn}") ` +
      `ON UPDATE CASCADE ON DELETE RESTRICT`,
  );
};

export default {
  up: async (queryInterface: QueryInterface): Promise<void> => {
    const sequelize = queryInterface.sequelize;

    const tenantFks = await getTenantFkConstraints(sequelize);
    for (const fk of tenantFks) {
      await alterFkToCascade(
        sequelize,
        fk.table_name,
        fk.constraint_name,
        fk.column_name,
        fk.referenced_table,
        fk.referenced_column,
      );
    }

    const tenantPartnerFks = await getTenantPartnerFkConstraints(sequelize);
    for (const fk of tenantPartnerFks) {
      await alterFkToCascade(
        sequelize,
        fk.table_name,
        fk.constraint_name,
        fk.column_name,
        fk.referenced_table,
        fk.referenced_column,
      );
    }
  },

  down: async (queryInterface: QueryInterface): Promise<void> => {
    const sequelize = queryInterface.sequelize;

    const tenantPartnerFks = await getTenantPartnerFkConstraints(sequelize);
    for (const fk of tenantPartnerFks) {
      await alterFkToRestrict(
        sequelize,
        fk.table_name,
        fk.constraint_name,
        fk.column_name,
        fk.referenced_table,
        fk.referenced_column,
      );
    }

    const tenantFks = await getTenantFkConstraints(sequelize);
    for (const fk of tenantFks) {
      await alterFkToRestrict(
        sequelize,
        fk.table_name,
        fk.constraint_name,
        fk.column_name,
        fk.referenced_table,
        fk.referenced_column,
      );
    }
  },
};
