import { StateCreator } from 'zustand';
import { persist } from 'zustand/middleware';

import { CURRENT_MODE, LIGHT_MODE } from '@/constants/global';
import { ThemeMode } from '@/types/global-types';
import { DarkModeState, StoreTypes } from '@/types/store-types';

const createDarkModeSlice: StateCreator<
  StoreTypes,
  [['zustand/immer', never], ['zustand/devtools', never]],
  [['zustand/persist', DarkModeState]],
  DarkModeState
> = persist(
  (set) => ({
    isDark: LIGHT_MODE,
    toggleTheme: (mode: ThemeMode) =>
      set((state) => {
        state.isDark = mode;
      }),
  }),
  {
    name: CURRENT_MODE,
  },
);

export default createDarkModeSlice;