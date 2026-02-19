// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
'use strict';

/** @type {import('sequelize-cli').Migration} */
import { QueryInterface } from 'sequelize';

const TENANT_UNIQUE_CONSTRAINTS = [
  {
    table: 'Components',
    oldConstraint: 'Components_name_instance_key',
    oldPartialIndex: 'components_name',
    newFullIndex: 'Components_tenantId_name_instance_key',
    newPartialIndex: 'components_tenantId_name',
    columns: ['tenantId', 'name', 'instance'],
    partialColumns: ['tenantId', 'name'],
  },
  {
    table: 'Variables',
    oldConstraint: 'Variables_name_instance_key',
    oldPartialIndex: 'variables_name',
    newFullIndex: 'Variables_tenantId_name_instance_key',
    newPartialIndex: 'variables_tenantId_name',
    columns: ['tenantId', 'name', 'instance'],
    partialColumns: ['tenantId', 'name'],
  },
];

export default {
  up: async (queryInterface: QueryInterface) => {
    for (const c of TENANT_UNIQUE_CONSTRAINTS) {
      await queryInterface.sequelize.query(`
        ALTER TABLE "${c.table}" DROP CONSTRAINT IF EXISTS "${c.oldConstraint}";
      `);
      await queryInterface.sequelize.query(`
        DROP INDEX IF EXISTS "${c.oldPartialIndex}";
      `);
      await queryInterface.sequelize.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS "${c.newFullIndex}"
          ON "${c.table}" (${c.columns.map((col) => `"${col}"`).join(', ')});
      `);
      await queryInterface.sequelize.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS "${c.newPartialIndex}"
          ON "${c.table}" (${c.partialColumns.map((col) => `"${col}"`).join(', ')})
          WHERE "instance" IS NULL;
      `);
    }
  },

  down: async (queryInterface: QueryInterface) => {
    for (const c of [...TENANT_UNIQUE_CONSTRAINTS].reverse()) {
      await queryInterface.sequelize.query(`
        DROP INDEX IF EXISTS "${c.newPartialIndex}";
      `);
      await queryInterface.sequelize.query(`
        DROP INDEX IF EXISTS "${c.newFullIndex}";
      `);
      await queryInterface.sequelize.query(`
        ALTER TABLE "${c.table}" ADD CONSTRAINT "${c.oldConstraint}" UNIQUE ("name", "instance");
      `);
      await queryInterface.sequelize.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS "${c.oldPartialIndex}" ON "${c.table}" ("name") WHERE "instance" IS NULL;
      `);
    }
  },
};
