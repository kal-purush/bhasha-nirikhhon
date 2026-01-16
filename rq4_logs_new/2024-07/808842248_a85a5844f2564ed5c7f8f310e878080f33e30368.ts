import { Test, TestingModule } from '@nestjs/testing';
import { GoogleCloudService } from './google-cloud.service';
import * as Axios from 'axios';

jest.mock('fs', () => {
  return {
    createWriteStream: () => {
      return {
        on: jest.fn().mockImplementation((event, handler) => {
          if (event === 'finish') {
            handler();
          } else if (event === 'error') {
            handler(new Error('error'));
          } else if (event === 'end') {
            handler();
          }
          return this;
        }),
      };
    },
    createReadStream: () => {
      return {
        on: jest.fn().mockImplementation((event, handler) => {
          if (event === 'finish') {
            handler();
          } else if (event === 'error') {
            handler(new Error('error'));
          } else if (event === 'end') {
            handler();
          }
          return this;
        }),
      };
    },
    unlinkSync: jest.fn(),
  };
});
jest.mock('@google-cloud/storage', () => {
  const mFile = {
    download: jest.fn().mockResolvedValue(['mockedFile']),
  };
  const mBucket = {
    upload: jest.fn().mockResolvedValue(['mockedFile']),
    file: jest.fn(() => mFile),
  };
  const mStorage = {
    bucket: jest.fn(() => mBucket),
  };
  return { Storage: jest.fn(() => mStorage) };
});

describe('GoogleCloudService', () => {
  let service: GoogleCloudService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [GoogleCloudService],
    }).compile();

    service = module.get<GoogleCloudService>(GoogleCloudService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('uploadToStorage', () => {
    beforeEach(() => {
      jest
        .spyOn(Axios, 'default')
        .mockResolvedValue({ data: { pipe: jest.fn() } });
    });

    it('ファイルアップロードができること', async () => {
      const fileExternalUrl = 'https://example.com/test.jpg';
      const fileName = 'test_image';
      const filePath = 'test_image.jpg';

      const result = await service.uploadToStorage(fileExternalUrl, fileName);
      expect(result).toEqual(filePath);
    });
  });

  describe('downloadFromStorage', () => {
    it('ファイルダウンロードができること', async () => {
      const filePath = 'test_image.jpg';

      const result = await service.downloadFromStorage(filePath);
      expect(result).toEqual('data:image/jpeg;base64,mockedFile');
    });
  });
});