import { BASE_URL } from './const';

export interface LoginRequest {
  username: string;
  password: string;
}

export const login = async (args: LoginRequest) => {
  const res = await fetch(`${BASE_URL}/admin/login`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      credentials: 'include',
    },
    body: JSON.stringify(args),
  });

  console.log(res.headers.get('Authorization'));
  console.log(res.headers.get('Set-Cookie'));
  //   return res.ok ? 'success' : 'fail';
};