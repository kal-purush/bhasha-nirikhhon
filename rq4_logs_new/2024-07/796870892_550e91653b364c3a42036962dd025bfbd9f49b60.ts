import { create } from "zustand";
import { AuthUserResponse } from "@/interfaces/user";

type AuthStoreState = {
  authUser: AuthUserResponse | null;
  saveAuthUser: (user: AuthUserResponse) => void;
  clearAuthUser: () => void;
};

const useAuthStore = create<AuthStoreState>((set) => ({
  authUser: null,
  saveAuthUser: (currentAuthUser: AuthUserResponse) =>
    set(() => ({
      authUser: currentAuthUser
    })),
  clearAuthUser: () =>
    set(() => ({
      authUser: null
    }))
}));

export default useAuthStore;