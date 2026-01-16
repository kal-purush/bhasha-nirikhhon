import { renderHook, act } from '@testing-library/react-hooks/dom';
import nock from 'nock';

import { useUser } from '../use-user';
import { getUser } from '../../api';

jest.mock('../../api');

describe('User-User', () => {
  const user = {
    id: 'TD0sIeaoz',
    email: 'person.one@example.com',
    firstName: 'Person1',
    lastName: 'One',
    role: 'user',
  };

  beforeAll(() => {
    nock.disableNetConnect();
  });

  afterAll(() => {
    nock.restore();
  });

  describe('indeterminate', () => {
    it('should return undefined', () => {
      (getUser as jest.Mock).mockImplementation(() => ({
        then: () => undefined,
      }));

      const {
        result: { current: user },
      } = renderHook(() => useUser());

      expect(user).toBeUndefined();
    });
  });

  describe('logged-out', () => {
    it('should return null eventually', async () => {
      (getUser as jest.Mock).mockImplementation(
        jest.requireActual('../../api').getUser
      );

      const scope = nock('http://localhost')
        .get('/api/user')
        .reply(401, { status: 'error' });

      await act(async () => {
        const hook = renderHook(() => useUser());

        await hook.waitForNextUpdate();

        expect(scope.isDone()).toBe(true);
        expect(hook.result.current).toBeNull();
      });
    });
  });

  describe('logged-in', () => {
    it('should return the user eventually', async () => {
      (getUser as jest.Mock).mockImplementation(
        jest.requireActual('../../api').getUser
      );

      const scope = nock('http://localhost')
        .get('/api/user')
        .reply(200, { status: 'ok', user });

      await act(async () => {
        const hook = renderHook(() => useUser());

        await hook.waitForNextUpdate();

        expect(scope.isDone()).toBe(true);
        expect(hook.result.current).toEqual(user);
      });
    });
  });
});