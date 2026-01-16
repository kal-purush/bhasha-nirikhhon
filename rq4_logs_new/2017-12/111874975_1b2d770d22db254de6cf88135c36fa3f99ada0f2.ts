import { Cache } from './cache';
import { CacheNotFoundError, CacheExpiredError } from '../public';

const sampleConfig = {
  'my-table': {
    'my-document': {
      expires: Date.now() + 1000,
      data: {
        path: true,
        path1: {
          path2: true,
          'path2a.path2b': true
        },
      }
    }
  },
  'expired-table': {
    'expired-document': {
      expires: Date.now() - 1,
      data: { path: true }
    }
  }
};

describe('Cache', () => {
  let cache: Cache;
  beforeEach(() => {
    cache = new Cache();
  });

  describe('#construction', () => {
    it('should set default expires', () => {
      cache = new Cache({ defaultExpires: 10 });
      expect(cache.defaultExpires).toBe(10000);   // in ms
    });
  });

  describe('#config', () => {
    it('should update config', () => {
      cache = new Cache({ defaultExpires: 20 });
      expect(cache.defaultExpires).toBe(20000);
    });
  });

  describe('#setCache', () => {
    it('should set config', () => {
      const data = { hello: 'world' };
      cache.setCache('my-table', 'my-document', data);
      expect(cache.cacheStore).toEqual(<any>jasmine.objectContaining({
        'my-table': {
          'my-document': {
            expires: jasmine.any(Number),
            data,
          }
        }
      }));
    });
  });

  describe('#getCache', () => {
    it('should get config (whole config)', () => {
      cache.cacheStore = sampleConfig;
      const result = cache.getCache('my-table', 'my-document');
      expect(result).toBe(sampleConfig['my-table']['my-document'].data);
    })

    it('should get config (string key)', () => {
      cache.cacheStore = sampleConfig;
      const result = cache.getCache('my-table', 'my-document', 'path1.path2');
      expect(result).toBeTruthy();
    });

    it('should get config (array key)', () => {
      cache.cacheStore = sampleConfig;
      const result = cache.getCache('my-table', 'my-document', ['path1', 'path2a.path2b']);
      expect(result).toBeTruthy();
    });

    it('should throw not found error', () => {
      try {
        cache.getCache('my-table', 'not-exist-document');
      } catch (error) {
        expect(error instanceof CacheNotFoundError).toBeTruthy();
      }
    });

    it('should throw expired error', () => {
      cache.cacheStore = sampleConfig;
      try {
        cache.getCache('expired-table', 'expired-document');
      } catch (error) {
        expect(error instanceof CacheExpiredError).toBeTruthy();
      }
    });
  });

  describe('#deleteCache', () => {
    it('should delete config', () => {
      cache.cacheStore = sampleConfig;
      cache.deleteCache('my-table', 'my-document');
      expect(cache.cacheStore['my-table']).toBeUndefined();
    });
  });

  describe('#cleanExpiredCache', () => {
    it('should clean up expired config', () => {
      cache.cacheStore = sampleConfig;
      cache.cleanExpiredCache();
      expect(cache.cacheStore).toEqual(jasmine.objectContaining({}));
    });
  })
});