import { PhotosApiRepo } from './nasa.api.repo';

describe('Given the service Api repo class', () => {
  describe('When create a new object of the class', () => {
    test('Then it should be able to call its method', async () => {
      global.fetch = jest.fn().mockResolvedValue({
        json: jest.fn().mockResolvedValue({ photos: [] }),
      });
      const repo = new PhotosApiRepo();
      expect(repo).toBeInstanceOf(PhotosApiRepo);
      const load = await repo.loadPhotos();
      expect(load).toEqual({ photos: [] });
    });
  });
});