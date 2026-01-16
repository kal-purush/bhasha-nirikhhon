import {Action} from 'redux';
import thunk, {ThunkDispatch} from 'redux-thunk';
import MockAdapter from 'axios-mock-adapter';
import {configureMockStore} from '@jedmao/redux-mock-store';
import {createAPI} from '../services/api';
import {APIRoute} from '../constants';
import {State} from '../types/state';
import { makeFakeFriend} from '../utils/mocks';
import { fetchCountFriends } from './api-actions-friends';
import { UserRole } from '../types/user';

describe('Async actions', () => {
  const api = createAPI();
  const mockAPI = new MockAdapter(api);
  const middlewares = [thunk.withExtraArgument(api)];

  const mockStore = configureMockStore<
      State,
      Action<string>,
      ThunkDispatch<State, typeof api, Action>
    >(middlewares);

  it('should dispatch fetchCountFriends', async () => {
    const makeFakeFriends = Array.from({length: 5}, () => makeFakeFriend());
    mockAPI
      .onGet(`${APIRoute.Coach}/friends/count`)
      .reply(200, makeFakeFriends);

    const store = mockStore();

    await store.dispatch(fetchCountFriends(UserRole.Coach));
    const actions = store.getActions().map(({type}) => type);

    expect(actions).toEqual([
      fetchCountFriends.pending.type,
      fetchCountFriends.fulfilled.type
    ]);
  });
});