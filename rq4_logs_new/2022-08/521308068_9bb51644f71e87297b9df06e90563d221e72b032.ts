import { isAuthenticatedState } from './../stores/auth';
import { tokenState } from '../stores/auth';
import { useSetRecoilState } from 'recoil';

export default function useAuth() {
  const setToken = useSetRecoilState(tokenState);
  const setIsAuthenticated = useSetRecoilState(isAuthenticatedState);

  const saveToken = (token: string) => {
    setToken(token);
    localStorage.setItem('token', token);
    setIsAuthenticated(true);
  };

  const initAuth = () => {
    const token = localStorage.getItem('token');
    if (token) {
      saveToken(token);
    }
  };

  return { saveToken, initAuth };
}