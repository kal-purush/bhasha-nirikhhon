import { create } from "zustand";

interface ModalStore {
  ishasProba: boolean;
  setHasProba: () => void;
}

export const useHasProbaStore = create<ModalStore>((set) => ({
  ishasProba: false,
  setHasProba: () => set({ ishasProba: true }),
}));