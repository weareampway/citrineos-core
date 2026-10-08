// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0

export function createWriteQueue(): <T>(operation: () => Promise<T>) => Promise<T> {
  let pendingWrites: Promise<void> = Promise.resolve();

  return <T>(operation: () => Promise<T>): Promise<T> => {
    const run = pendingWrites.then(operation);
    // The caller applies the returned config before the next write starts.
    pendingWrites = run.then(
      () => waitForCaller(),
      () => waitForCaller(),
    );
    return run;
  };
}

function waitForCaller(): Promise<void> {
  return new Promise((resolve) => {
    setImmediate(resolve);
  });
}
