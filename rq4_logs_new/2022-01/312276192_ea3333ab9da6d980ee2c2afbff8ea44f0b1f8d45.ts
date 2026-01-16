import { MockSierraClient } from '@weco/sierra-client';
import { Auth0User } from '@weco/auth0-client';
import verify from '../src/verify';

const mockSierraClient = new MockSierraClient();
jest.mock('@weco/sierra-client', () => {
  const actualModule = jest.requireActual('@weco/sierra-client');
  return {
    ...actualModule,
    HttpSierraClient: function () {
      return mockSierraClient;
    },
  };
});

describe('email verification script', () => {
  afterEach(() => {
    mockSierraClient.reset();
  });

  it('adds a verification note to a patron record', (done) => {
    const testRecord = MockSierraClient.randomPatronRecord();
    mockSierraClient.addPatron(testRecord);

    const callback = (
      error?: NodeJS.ErrnoException | null,
      data?: Auth0User
    ) => {
      expect(error).toBe(null);
      expect(data?.email_verified).toBe(true);

      const updatedRecord = mockSierraClient.get(testRecord.recordNumber);
      expect(updatedRecord?.verifiedEmails).toContain(testRecord.email);
      done();
    };
    verify(testRecord.email, callback);
  });

  it('throws an error if no patron with that email address exists', (done) => {
    const callback = (
      error?: NodeJS.ErrnoException | null,
      data?: Auth0User
    ) => {
      expect(error).not.toBe(null);
      expect(data).toBeUndefined();
      done();
    };
    verify('doesnot@exist.com', callback);
  });
});