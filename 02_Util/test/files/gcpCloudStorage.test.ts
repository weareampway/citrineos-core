// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
import type { SystemConfig } from '@citrineos/base';
import { Storage } from '@google-cloud/storage';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { GcpCloudStorage } from '../../src';

vi.mock('@google-cloud/storage', () => {
  const mockFile = {
    save: vi.fn(),
    exists: vi.fn(),
    download: vi.fn(),
    getMetadata: vi.fn(),
  };

  const mockBucket = {
    file: vi.fn(() => mockFile),
  };

  const mockStorage = {
    bucket: vi.fn(() => mockBucket),
    createBucket: vi.fn(),
  };

  return {
    Storage: vi.fn(() => mockStorage),
  };
});

describe('GcpCloudStorage', () => {
  let gcpStorage: GcpCloudStorage;
  let mockStorageInstance: any;
  let mockBucket: any;
  let mockFile: any;

  const mockConfig = {
    projectId: 'test-project',
  };

  const mockSystemConfig: SystemConfig = {
    modules: {},
    util: {},
  } as SystemConfig;

  beforeEach(() => {
    vi.clearAllMocks();

    // Get references to mocked instances
    mockStorageInstance = new Storage();
    mockBucket = mockStorageInstance.bucket();
    mockFile = mockBucket.file();

    gcpStorage = new GcpCloudStorage(mockConfig, 'config.json', 'test-bucket');
  });

  describe('constructor', () => {
    it('should throw error if config is missing', () => {
      expect(() => new GcpCloudStorage(null as any, 'config.json')).toThrow(
        'GCP Cloud Storage config missing.',
      );
    });

    it('should initialize with provided config', () => {
      expect(gcpStorage).toBeInstanceOf(GcpCloudStorage);
      expect(Storage).toHaveBeenCalled();
    });
  });

  describe('saveFile', () => {
    const fileName = 'test-file.txt';
    const content = Buffer.from('test content');

    it('should save file successfully', async () => {
      mockFile.save.mockResolvedValue(undefined);

      const result = await gcpStorage.saveFile(fileName, content);

      expect(result).toBe(fileName);
      expect(mockStorageInstance.bucket).toHaveBeenCalledWith('test-bucket');
      expect(mockBucket.file).toHaveBeenCalledWith(fileName);
      expect(mockFile.save).toHaveBeenCalledWith(content, {
        contentType: 'application/octet-stream',
        resumable: false,
      });
    });

    it('should use custom bucket if filePath is provided', async () => {
      mockFile.save.mockResolvedValue(undefined);

      await gcpStorage.saveFile(fileName, content, 'custom-bucket');

      expect(mockStorageInstance.bucket).toHaveBeenCalledWith('custom-bucket');
    });

    it('should create bucket and retry if bucket not found', async () => {
      const notFoundError = { code: 404, message: 'Not Found' };
      mockFile.save.mockRejectedValueOnce(notFoundError).mockResolvedValueOnce(undefined);
      mockStorageInstance.createBucket.mockResolvedValue(undefined);

      const result = await gcpStorage.saveFile(fileName, content);

      expect(result).toBe(fileName);
      expect(mockStorageInstance.createBucket).toHaveBeenCalledWith('test-bucket');
      expect(mockFile.save).toHaveBeenCalledTimes(2);
    });

    it('should throw error if save fails with non-404 error', async () => {
      const error = new Error('Network error');
      mockFile.save.mockRejectedValue(error);

      await expect(gcpStorage.saveFile(fileName, content)).rejects.toThrow('Network error');
      expect(mockStorageInstance.createBucket).not.toHaveBeenCalled();
    });

    it('should handle "No such object" error message', async () => {
      const notFoundError = { message: 'No such object: bucket-name' };
      mockFile.save.mockRejectedValueOnce(notFoundError).mockResolvedValueOnce(undefined);
      mockStorageInstance.createBucket.mockResolvedValue(undefined);

      const result = await gcpStorage.saveFile(fileName, content);

      expect(result).toBe(fileName);
      expect(mockStorageInstance.createBucket).toHaveBeenCalled();
    });
  });

  describe('getFile', () => {
    const fileId = 'test-file.txt';
    const fileContent = 'test content';

    it('should return file content as string', async () => {
      mockFile.exists.mockResolvedValue([true]);
      mockFile.download.mockResolvedValue([Buffer.from(fileContent)]);

      const result = await gcpStorage.getFile(fileId);

      expect(result).toBe(fileContent);
      expect(mockStorageInstance.bucket).toHaveBeenCalledWith('test-bucket');
      expect(mockBucket.file).toHaveBeenCalledWith(fileId);
      expect(mockFile.exists).toHaveBeenCalled();
      expect(mockFile.download).toHaveBeenCalled();
    });

    it('should return undefined if file does not exist', async () => {
      mockFile.exists.mockResolvedValue([false]);

      const result = await gcpStorage.getFile(fileId);

      expect(result).toBeUndefined();
      expect(mockFile.download).not.toHaveBeenCalled();
    });

    it('should use custom bucket if filePath is provided', async () => {
      mockFile.exists.mockResolvedValue([true]);
      mockFile.download.mockResolvedValue([Buffer.from(fileContent)]);

      await gcpStorage.getFile(fileId, 'custom-bucket');

      expect(mockStorageInstance.bucket).toHaveBeenCalledWith('custom-bucket');
    });

    it('should return undefined if 404 error is thrown', async () => {
      const notFoundError = { code: 404 };
      mockFile.exists.mockRejectedValue(notFoundError);

      const result = await gcpStorage.getFile(fileId);

      expect(result).toBeUndefined();
    });

    it('should return undefined if "Not Found" error is thrown', async () => {
      const notFoundError = { message: 'Not Found: file does not exist' };
      mockFile.exists.mockRejectedValue(notFoundError);

      const result = await gcpStorage.getFile(fileId);

      expect(result).toBeUndefined();
    });

    it('should throw error for non-404 errors', async () => {
      const error = new Error('Permission denied');
      mockFile.exists.mockRejectedValue(error);

      await expect(gcpStorage.getFile(fileId)).rejects.toThrow('Permission denied');
    });
  });

  describe('fetchConfig', () => {
    it('should fetch and parse config successfully', async () => {
      const configString = JSON.stringify(mockSystemConfig);
      mockFile.getMetadata.mockResolvedValue([{ generation: '1' }]);
      mockFile.download.mockResolvedValue([Buffer.from(configString)]);

      const result = await gcpStorage.fetchConfig();

      expect(result).toEqual(mockSystemConfig);
      expect(mockBucket.file).toHaveBeenCalledWith('config.json');
      expect(mockBucket.file).toHaveBeenCalledWith('config.json', { generation: '1' });
    });

    it('should return null if config file does not exist', async () => {
      mockFile.getMetadata.mockRejectedValue({ code: 404 });

      const result = await gcpStorage.fetchConfig();

      expect(result).toBeNull();
    });

    it('should return null if 404 error occurs', async () => {
      const notFoundError = { code: 404 };
      mockFile.getMetadata.mockRejectedValue(notFoundError);

      const result = await gcpStorage.fetchConfig();

      expect(result).toBeNull();
    });

    it('should return null if "could not find" error occurs', async () => {
      const notFoundError = { message: 'could not find config file' };
      mockFile.getMetadata.mockRejectedValue(notFoundError);

      const result = await gcpStorage.fetchConfig();

      expect(result).toBeNull();
    });

    it('should throw error for non-404 errors', async () => {
      const error = new Error('Network timeout');
      mockFile.getMetadata.mockRejectedValue(error);

      await expect(gcpStorage.fetchConfig()).rejects.toThrow('Network timeout');
    });

    it('retries when the pinned generation disappears', async () => {
      mockFile.getMetadata
        .mockResolvedValueOnce([{ generation: '1' }])
        .mockResolvedValueOnce([{ generation: '2' }]);
      mockFile.download
        .mockRejectedValueOnce({ code: 404 })
        .mockResolvedValueOnce([Buffer.from(JSON.stringify(mockSystemConfig))]);

      const result = await gcpStorage.fetchConfig();

      expect(result).toEqual(mockSystemConfig);
      expect(mockFile.getMetadata).toHaveBeenCalledTimes(2);
      expect(mockBucket.file).toHaveBeenCalledWith('config.json', { generation: '1' });
      expect(mockBucket.file).toHaveBeenCalledWith('config.json', { generation: '2' });
    });

    it('should handle invalid JSON gracefully', async () => {
      mockFile.getMetadata.mockResolvedValue([{ generation: '1' }]);
      mockFile.download.mockResolvedValue([Buffer.from('invalid json{')]);

      await expect(gcpStorage.fetchConfig()).rejects.toThrow();
    });
  });

  describe('saveConfig', () => {
    const storedConfig = {
      modules: {},
      util: {
        networkConnection: {
          websocketServers: [
            { id: '1', host: 'localhost', port: 9000, tenantId: 1 },
            { id: '2', host: 'localhost', port: 9002, tenantId: 2 },
          ],
        },
      },
    } as SystemConfig;

    beforeEach(() => {
      mockFile.getMetadata.mockResolvedValue([{ generation: '4' }]);
      mockFile.download.mockResolvedValue([Buffer.from(JSON.stringify(storedConfig))]);
    });

    it('should save config successfully', async () => {
      mockFile.save.mockResolvedValue(undefined);

      await gcpStorage.saveConfig(storedConfig);

      const saved = JSON.parse(mockFile.save.mock.calls[0][0].toString('utf-8'));
      expect(saved).toEqual(storedConfig);
      expect(mockFile.save).toHaveBeenCalledWith(expect.any(Buffer), {
        contentType: 'application/octet-stream',
        resumable: false,
        preconditionOpts: { ifGenerationMatch: '4' },
      });
    });

    it('keeps a stored websocket server that the incoming config does not contain', async () => {
      mockFile.save.mockResolvedValue(undefined);
      const incoming = structuredClone(storedConfig);
      incoming.util.networkConnection.websocketServers = [
        { ...storedConfig.util.networkConnection.websocketServers[0], host: '10.0.0.8' },
      ];

      await gcpStorage.saveConfig(incoming);

      const saved = JSON.parse(mockFile.save.mock.calls[0][0].toString('utf-8'));
      expect(saved.util.networkConnection.websocketServers).toEqual([
        { ...storedConfig.util.networkConnection.websocketServers[0], host: '10.0.0.8' },
        storedConfig.util.networkConnection.websocketServers[1],
      ]);
    });

    it('should create bucket and retry if bucket not found', async () => {
      const notFoundError = { code: 404 };
      mockFile.save.mockRejectedValueOnce(notFoundError).mockResolvedValueOnce(undefined);
      mockStorageInstance.createBucket.mockResolvedValue(undefined);

      await gcpStorage.saveConfig(storedConfig);

      expect(mockStorageInstance.createBucket).toHaveBeenCalledWith('test-bucket');
      expect(mockFile.save).toHaveBeenCalledTimes(2);
    });

    it('should throw error if save fails with non-404 error', async () => {
      const error = new Error('Insufficient permissions');
      mockFile.save.mockRejectedValue(error);

      await expect(gcpStorage.saveConfig(storedConfig)).rejects.toThrow('Insufficient permissions');
    });

    it('should properly format JSON with indentation', async () => {
      mockFile.save.mockResolvedValue(undefined);

      await gcpStorage.saveConfig(storedConfig);

      const expectedContent = Buffer.from(JSON.stringify(storedConfig, null, 2));
      expect(mockFile.save).toHaveBeenCalledWith(expectedContent, {
        contentType: 'application/octet-stream',
        resumable: false,
        preconditionOpts: { ifGenerationMatch: '4' },
      });
    });
  });
});
