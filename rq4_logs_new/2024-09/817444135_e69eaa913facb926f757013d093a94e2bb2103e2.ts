import axios from "axios";
import { create } from "zustand";
import { UserData } from "../../types/accordion.ts";

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
const BACKEND_URL = import.meta.env.VITE_BACKEND_URL;

interface UserState {
  user: UserData | null;
  isLoading: boolean;
  getUser: () => Promise<UserData | null>;
  setUser: (user: UserData | null) => void;
}

export const useUserStore = create<UserState>((set) => ({
  user: null,
  isLoading: true,
  getUser: async () => {
    set({ isLoading: true });
    try {
      const response = await axios.get(`${BACKEND_URL}/users/me`, {
        withCredentials: true,
      });
      set({ user: response.data, isLoading: false });
      return response.data;
    } catch (error) {
      console.error("Error fetching user:", error);
      set({ user: null, isLoading: false });
      return null;
    }
  },
  setUser: (user) => set({ user }),
}));