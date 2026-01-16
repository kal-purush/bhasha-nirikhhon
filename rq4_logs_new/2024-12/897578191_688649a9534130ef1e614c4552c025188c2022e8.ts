import { devtools, persist } from "zustand/middleware";
import { StateCreator, create } from "zustand";

interface ThemeState {
  theme        : boolean;
  menuIsOpen   : boolean;
  setTheme     : (theme: boolean) => void;
  setMenuIsOpen: (value: boolean) => void;
}

const storeApi: StateCreator<ThemeState> = (set) => ({
  theme: false,
  menuIsOpen: false,

  setTheme: (theme: boolean) => set({ theme }),
  setMenuIsOpen: (value: boolean) => set({ menuIsOpen: value }),
});

export const useThemeStore = create<ThemeState>()(
  devtools(
    persist(storeApi, {
      name: "theme-ui",
      partialize: (state) => ({ theme: state.theme }),
    })
  )
);