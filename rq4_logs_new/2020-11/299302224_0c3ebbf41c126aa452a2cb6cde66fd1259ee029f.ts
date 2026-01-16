import { createReducerContext } from '@/lib/react/create-reducer-context';
import { User } from '@/types/user';

export const [AuthContextProvider, AuthContextConsumer, useAuthContext] = createReducerContext<AuthState, AuthAction>(
  { isAuthenticated: false },
  (state, action) => {
    switch (action.type) {
      case 'LOG_IN':
        const { refreshToken, ...rest } = action.payload;
        localStorage.setItem('refreshToken', refreshToken);
        return { ...state, isAuthenticated: true, ...rest };

      case 'LOG_OUT':
        localStorage.removeItem('refreshToken');
        return { ...state, isAuthenticated: false, token: undefined, user: undefined };
    }
  },
);

interface LoggedInState {
  isAuthenticated: true;
  token: string;
  user: Pick<User, 'id' | 'username'>;
}

interface LoggedOutState {
  isAuthenticated: false;
}

type AuthState = LoggedInState | LoggedOutState;

type AuthAction =
  | { type: 'LOG_IN'; payload: Pick<LoggedInState, 'token' | 'user'> & { refreshToken: string } }
  | { type: 'LOG_OUT' };