import { InitialStateStructure } from '../hooks/use.photo.mars';

export const mockState = {
  photos: [],
  actualPhoto: {
    id: 1,
    sol: 1,
    camera_id: 1,
    camera_name: 'test',
    camera_rover_id: 1,
    camera_full_name: '',
    img_src:
      'https://mars.jpl.nasa.gov/msl-raw-images/proj/msl/redops/ods/surface/sol/01000/opgs/edr/fcam/FLB_486265257EDR_F0481570FHAZ00323M_.JPG',
    earth_date: '',
    rover_id: 1,
    rover_name: '',
    rover_landing_date: '',
    rover_launch_date: '',
    rover_status: '',
    apiOrigin: '',
    isFavorite: false,
    favoriteName: '',
  },
  privatePhotos: [],
} as InitialStateStructure;

export const mockContext = {
  state: mockState,
  loadPhotos: jest.fn(),
  actualCard: jest.fn(),
  loadPrivatePhotos: jest.fn(),
  createPhoto: jest.fn(),
};