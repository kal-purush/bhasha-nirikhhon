import { USER_LOGIN, USER_SIGNUP } from '../src/graphql/mutations';

export const testToken = 'add get token method';
export const signup = {
  request: {
    query: USER_SIGNUP,
    variables: {
      email: 'jonathan@example.com', firstName: 'jonathan', lastName: 'doe', password: 'Pasword1234', userName: 'jonathanDoe',
    },
  },
  result: {
    data: {
      signUp: {
        token: testToken,
        user: {
          firstName: 'John',
          lastName: 'Doe',
        },
      },
    },
  },
};
export const login = {
  request: {
    query: USER_LOGIN,
    variables: { email: 'jane.doe@example.com', password: 'applicationUser1' },
  },
  result: {
    data: {
      signIn: {
        token: testToken,
        user: {
          firstName: 'John',
          lastName: 'Doe',
        },
      },
    },
  },
};