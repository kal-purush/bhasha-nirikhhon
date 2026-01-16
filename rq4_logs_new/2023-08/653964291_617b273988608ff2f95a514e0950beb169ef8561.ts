import {Action} from 'redux';
import thunk, {ThunkDispatch} from 'redux-thunk';
import MockAdapter from 'axios-mock-adapter';
import {configureMockStore} from '@jedmao/redux-mock-store';
import {createAPI} from '../services/api';
import {registerCoach, deleteCoachFriend} from './api-actions-coach';
import {APIRoute} from '../constants';
import {State} from '../types/state';
import { makeFakeUser, makeFakeUserFullInfo } from '../utils/mocks';

describe('Async actions', () => {
  const api = createAPI();
  const mockAPI = new MockAdapter(api);
  const middlewares = [thunk.withExtraArgument(api)];

  const mockStore = configureMockStore<
      State,
      Action<string>,
      ThunkDispatch<State, typeof api, Action>
    >(middlewares);

  it('should authorization status is «auth» when server return 200', async () => {
    const store = mockStore();
    mockAPI
      .onPost(APIRoute.Login)
      .reply(200, []);

    expect(store.getActions()).toEqual([]);

    await store.dispatch(registerCoach());

    const actions = store.getActions().map(({type}) => type);

    expect(actions).toEqual([
      checkAuthAction.pending.type,
      checkAuthAction.fulfilled.type
    ]);
  });


  it('should dispatch Load_User when GET /user', async () => {
    const fakeUser = makeFakeUser();
    mockAPI
      .onGet(APIRoute.CheckUser)
      .reply(200, fakeUser);

    const store = mockStore();

    await store.dispatch(fetchUser());

    const actions = store.getActions().map(({type}) => type);

    expect(actions).toEqual([
      fetchUser.pending.type,
      fetchUser.fulfilled.type
    ]);
  });

  it('should dispatch Load_Users when GET /users', async () => {
    const fakeUsers = Array.from({length: 5}, () => makeFakeUserFullInfo());
    mockAPI
      .onGet(APIRoute.Users)
      .reply(200, fakeUsers);

    const store = mockStore();

    await store.dispatch(fetchUserCatalog());
    console.log(store)
    const actions = store.getActions().map(({type}) => type);

    expect(actions).toEqual([
      fetchUserCatalog.pending.type,
      fetchUserCatalog.fulfilled.type
    ]);
  });
});