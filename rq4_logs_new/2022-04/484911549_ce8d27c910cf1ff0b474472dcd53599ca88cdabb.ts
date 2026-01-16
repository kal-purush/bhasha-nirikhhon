/* eslint-disable import/no-extraneous-dependencies */
import request from 'supertest';
import type { Spy } from '@erdtrade/types';
import { Server } from '@erdtrade/api';

beforeEach((done) => done());

afterEach((done) => {
  jest.clearAllMocks();
  Server.close();
  done();
});

afterAll(() => jest.restoreAllMocks());

describe('general log tests', () => {
  const consoleLogMock: Spy = jest.spyOn(console, 'log').mockImplementation();
  const consoleErrorMock: Spy = jest.spyOn(console, 'error').mockImplementation();

  test('the logger should be called independent of the query', async () => {
    await request(Server).get('/armors?name="All-knowing Armor"').expect(200);
    expect(consoleLogMock).toHaveBeenCalled();
  });

  test('for a successful query, an info log should be made', async () => {
    await request(Server).get('/armors?name="All-knowing Armor"').expect(200);
    expect(consoleLogMock).toHaveBeenCalledTimes(1);
  });

  test('for an unsuccessful query, an info log and an error log should be made', async () => {
    await request(Server).get('/armors?name=randomstring').expect(400);
    expect(consoleLogMock).toHaveBeenCalledTimes(1);
    expect(consoleErrorMock).toHaveBeenCalledTimes(1);
  });

  test('the logger should provide details regarding the request made and its output', async () => {
    await request(Server).get('/armors?name=randomstring').expect(400);
    expect(consoleErrorMock).toHaveBeenCalledWith(
      expect.stringContaining('[ERROR] [ERAPI] Could not search for randomstring in armor')
    );

    consoleLogMock.mockClear();

    await request(Server).get('/armors?name="All-knowing Armor"').expect(200);
    expect(consoleLogMock).toHaveBeenCalledWith(
      expect.stringContaining('Request made on armor for "All-knowing Armor"')
    );
  });
});