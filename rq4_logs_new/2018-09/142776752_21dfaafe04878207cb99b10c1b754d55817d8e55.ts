import fetch from 'unfetch';

jest.mock('unfetch', () => jest.fn());

import Cmd from './';

describe('Cmd', () => {
  describe('checkGraphqlResponse', () => {
    describe('when the query is valid and the response is ok', () => {
      it('should return the data from the query', async () => {
        const data = { something: ['c'] };
        const mockRes = {
          ok: true,
          json: jest.fn(_ => Promise.resolve({ errors: null, data }))
        };

        const returnVal = await Cmd.checkGraphQlResponse(mockRes);

        expect(returnVal).toEqual(data);
      });
    });

    describe('when there is some error', () => {
      it('should return the error message', async () => {
        const errors = [{ message: 'There was an error' }];
        const mockRes = {
          ok: false,
          json: jest.fn(_ => Promise.resolve({ errors }))
        };

        try {
          await Cmd.checkGraphQlResponse(mockRes);
        } catch (error) {
          expect(error).toBeDefined;
          expect(error.message).toEqual('There was an error');
        }
      });
    });
  });

  describe('buildGraphQlPayload', () => {
    afterEach(() => {
      window.sessionStorage.getItem.mockReset();
    });

    it('should include an Authorization header when specified', () => {
      window.sessionStorage.getItem.mockImplementationOnce(_ => 'random token');
      const payload = Cmd.buildGraphQlPayload('', true);

      expect(window.sessionStorage.getItem).toHaveBeenCalledTimes(1);
      expect(payload.headers).toEqual(
        expect.objectContaining({
          Authorization: expect.any(String)
        })
      );
    });

    it('should **NOT** include an Authorization header if not specified', () => {
      const payload = Cmd.buildGraphQlPayload('');

      expect(window.sessionStorage.getItem).toHaveBeenCalledTimes(0);
      expect(payload.headers).not.toEqual(
        expect.objectContaining({
          Authorization: expect.any(String)
        })
      );
    });
  });

  describe('mutation', () => {
    let check;
    let build;

    beforeEach(() => {
      check = Cmd.checkGraphQlResponse;
      build = Cmd.buildGraphQlPayload;

      Cmd.checkGraphQlResponse = jest.fn(x => x);
      Cmd.buildGraphQlPayload = jest.fn();
    });

    afterEach(() => {
      Cmd.checkGraphQlResponse = check;
      Cmd.buildGraphQlPayload = build;
    });

    it('should be able to make an authorized request', async () => {
      fetch.mockImplementationOnce(() => Promise.resolve({ data: 'data' }));

      const res = await Cmd.mutation('{ some { query { data } } }', true);

      expect(Cmd.buildGraphQlPayload).toHaveBeenCalledTimes(1);
      expect(Cmd.checkGraphQlResponse).toHaveBeenCalledTimes(1);
      expect(res).toEqual({ data: 'data' });
    });

    it('should return the error if it catches one', async () => {
      fetch.mockImplementationOnce(() => Promise.reject({ errors: 'error' }));

      const res = await Cmd.mutation('{ some { query { data } } }', true);

      expect(Cmd.buildGraphQlPayload).toHaveBeenCalledTimes(1);
      /**
       * Promise is being rejected on the fetch layer, so we wouldn't be able
       * to check the response of the request if it itself threw an error.
       */
      expect(Cmd.checkGraphQlResponse).toHaveBeenCalledTimes(0);
      expect(res).toEqual({ errors: 'error' });
    });
  });
});