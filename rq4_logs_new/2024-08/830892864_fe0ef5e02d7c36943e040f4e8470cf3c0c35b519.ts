import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface EditState {
  isEditing: boolean;
  startEditing: () => void;
  stopEditing: () => void;
  toggleEditing: () => void;
  resetEditing: () => void;
}

const useEditStore = create(
  persist<EditState>(
    (set) => ({
      isEditing: false,
      startEditing: () => set({ isEditing: true }),
      stopEditing: () => set({ isEditing: false }),
      toggleEditing: () => set((state) => ({ isEditing: !state.isEditing })),
      resetEditing: () => set({ isEditing: false }),
    }),
    {
      name: 'edit-storage',
    },
  ),
);

export default useEditStore;