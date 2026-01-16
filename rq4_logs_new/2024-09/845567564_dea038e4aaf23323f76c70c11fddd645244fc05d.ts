export const textMap = {
  login: '로그인',
  register: '회원가입',
};

export type AuthType = {
  type: 'register' | 'login';
};

export type LoginType = {
  id: string;
  password: string;
};