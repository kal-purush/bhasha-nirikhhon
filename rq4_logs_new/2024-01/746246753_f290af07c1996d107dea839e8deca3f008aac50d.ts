import { create } from "zustand";

type User = {
  username: string;
  id: string;
  email: string;
};

type Store = {
  user: User | null;
  setUser: (user: User) => void;
  logout: () => void;
};

export const useStore = create<Store>((set) => ({
  user: null,
  setUser: (user) => set({ user }),
  logout: () => set({ user: null }),
}));