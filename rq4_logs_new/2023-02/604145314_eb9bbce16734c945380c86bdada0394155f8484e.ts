import { PrivateApiRepo } from './private.api.repo';

describe('Given the service Private repo class', () => {
  describe('When create a new object of the class', () => {
    test('Then it should be able to call its method', async () => {
      global.fetch = jest.fn().mockResolvedValue({
        json: jest.fn().mockResolvedValue({ photos: [] }),
      });
      const repo = new PrivateApiRepo();
      expect(repo).toBeInstanceOf(PrivateApiRepo);
      const load = await repo.loadPrivatePhotos();
      expect(load).toEqual({ photos: [] });
    });
  });
});