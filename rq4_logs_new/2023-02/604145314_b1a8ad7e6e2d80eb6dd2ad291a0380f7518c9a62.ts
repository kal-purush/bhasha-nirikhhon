import { rootObject } from './photos.actions';
import { loadPhotosCreator } from './photos.actions.creator';

describe('Given the photos actions creator', () => {
  describe('When called the function', () => {
    test('Then it should return an object with the proper type and payload', () => {
      const mockPhotoStructure = [
        {
          id: 1,
          sol: 1,
          camera_id: 3,
          camera_name: 'test',
          camera_rover_id: 6,
          camera_full_name: 'test',
          img_src: 'test',
          earth_date: 'test',
          rover_id: 1,
          rover_name: 'test',
          rover_landing_date: 'test',
          rover_launch_date: 'test',
          rover_status: 'test',
          apiOrigin: 'APIPublic',
          isFavorite: false,
          favoriteName: 'test',
        },
      ];
      const element = loadPhotosCreator(mockPhotoStructure);
      const result = { type: rootObject.load, payload: mockPhotoStructure };
      expect(element).toEqual(result);
    });
  });
});