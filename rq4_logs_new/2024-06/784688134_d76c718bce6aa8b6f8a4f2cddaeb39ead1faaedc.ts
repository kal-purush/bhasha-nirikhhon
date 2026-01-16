import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { useState, useEffect } from 'react';
import { Item } from '@/types/item';

export type UIStore = {
  savedItems: Item['id'][];
  menuOpen: boolean;
  saveItem: (data: Item['id']) => void;
  removeItem: (data: Item['id']) => void;
  setMenuOpen: () => void;
};

const useUIStore = create<UIStore>()(
  persist(
    (set) => ({
      savedItems: [],
      menuOpen: false,
      setMenuOpen: () => set((state) => ({ menuOpen: !state.menuOpen })),
      saveItem: (data: Item['id']) => {
        set((state) => ({
          savedItems: state.savedItems.includes(data)
            ? state.savedItems
            : [...state.savedItems, data],
        }));
      },
      removeItem: (data: Item['id']) => {
        set((state) => ({
          savedItems: state.savedItems.filter((item) => item != data),
        }));
      },
    }),
    {
      name: 'UIStore',
    },
  ),
);

const usePersistedUIStore = <F>(callback: (state: UIStore) => F) => {
  const result = useUIStore(callback) as F;
  const [data, setData] = useState<F>();

  useEffect(() => {
    setData(result);
  }, [result]);

  return data;
};

export default usePersistedUIStore;