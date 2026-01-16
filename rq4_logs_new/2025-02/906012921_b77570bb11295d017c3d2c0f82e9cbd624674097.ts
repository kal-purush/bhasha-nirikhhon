import { create } from 'zustand';

interface UserState {
  name: string;
  email: string;
  studentId: number;
  term: string;
  githubId: string;
  imgUrl: string;
  setUser: (user: Partial<UserState>) => void;
  resetUser: () => void;
}

export const useUserStore = create<UserState>((set) => ({
  name: sessionStorage.getItem('name') || '',
  email: sessionStorage.getItem('email') || '',
  studentId: Number(sessionStorage.getItem('studentId')) || 0,
  term: sessionStorage.getItem('term') || '',
  githubId: sessionStorage.getItem('githubId') || '',
  imgUrl: sessionStorage.getItem('imgUrl') || '',

  setUser: (user) => {
    set(user);
    Object.entries(user).forEach(([key, value]) => {
      sessionStorage.setItem(key, value as string);
    });
  },

  resetUser: () => {
    set({
      name: '',
      email: '',
      studentId: 0,
      term: '',
      githubId: '',
      imgUrl: '',
    });
  },
}));