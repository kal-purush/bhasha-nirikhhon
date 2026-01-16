import moxios from 'moxios';

import { storeFactory } from '../../utils';
import { StoreState } from '../reducers/rootReducer';
import { fetchUser } from '.';
import { User } from './userActions';

describe('fetchUser action creator', () => {
  beforeEach(() => {
    moxios.install();
  });
  afterEach(() => {
    moxios.uninstall();
  });
  test('adds fetched user data to state', () => {
    const fetchedData: User = {
      id: '123',
      photo: 'photo.jpg',
      role: 'user',
      intro: 'I am a user',
      education: [['A degree', 'B degree']],
      employmentHistory: [['Company C', 'Company D']],
      skills: [
        {
          img: 'imgage.jpg',
          title: 'Title 1',
          description: 'Description 1',
          id: '456',
        },
      ],
      name: 'username',
      email: 'username@xyz.com',
      projects: [
        {
          projectDetail: {
            details: ['detail 1'],
            subTitle: 'subtitle 1',
            gitRepoLinks: ['link 1'],
            demoLink: 'link 2',
            note: 'note 1',
          },
          img: 'image1.jpg',
          title: 'Title 1',
          description: 'Description 1',
          id: '123',
        },
      ],
    };

    const initialState: StoreState = {
      user: {
        user: {
          id: '',
          photo: '',
          role: '',
          intro: '',
          education: [],
          employmentHistory: [],
          skills: [],
          name: '',
          email: '',
          projects: [],
        },
        isLoadingUser: false,
        errorMessage: '',
      },
    };

    const store = storeFactory(initialState);

    moxios.wait(() => {
      const request = moxios.requests.mostRecent();
      request.respondWith({
        status: 200,
        response: fetchedData,
      });
    });

    return store.dispatch<any>(fetchUser()).then(() => {
      const newState: StoreState = store.getState();
      expect(newState.user.user).toBe(fetchedData);
    });
  });
});