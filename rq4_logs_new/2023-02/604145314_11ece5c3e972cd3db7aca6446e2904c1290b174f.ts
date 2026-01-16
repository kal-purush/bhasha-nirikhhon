import { photosReducer } from './photos.reducer';
import { MarsPhotoStructure } from '../models/marsPhoto';
import { rootObject } from './photos.actions';

describe('Given the photosReducer fuction', () => {
  const mockPhotos = [
    {
      id: 1,
      sol: 'test',
      camera_id: 'test',
      camera_name: 4,
      camera_rover_id: 6,
      camera_full_name: 'test',
      img_src: 'test',
      earth_date: 'test',
      rover_id: 'test',
      rover_name: 'test',
      rover_landing_date: 'test',
      rover_launch_date: 'test',
      rover_status: 'test',
    },
  ] as unknown as MarsPhotoStructure[];

  const mockloadPhotosCreator = {
    type: rootObject.load,
    payload: mockPhotos,
  };

  describe('When the action is load', () => {
    test('Then it should returned the state', async () => {
      const photo = photosReducer(mockPhotos, mockloadPhotosCreator);
      expect(photo).toEqual(mockPhotos);
    });
  });
});