// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
// SPDX-License-Identifier: Apache-2.0

import { AsyncLocalStorage } from 'node:async_hooks';

// A router delivery can span several cache reads before it writes to a WebSocket.
// Only its original AMQP channel owns it until that channel retires and RabbitMQ requeues it.
export const stationDeliveryIsCurrent = new AsyncLocalStorage<() => boolean>();
