import { create } from 'zustand';
import { GooseLocation } from '../interfaces/gooseLocation';

interface GooseLocationsState {
  gooseLocations: GooseLocation[];
  setGooseLocations: (locations: GooseLocation[]) => void;
  addGooseLocation: (location: GooseLocation) => void;
  removeGooseLocation: (id: string) => void;
}

const useGooseLocationStore = create<GooseLocationsState>()((set) => ({
  gooseLocations: [],
  setGooseLocations: (locations: GooseLocation[]) => set({ gooseLocations: locations }),
  addGooseLocation: (location: GooseLocation) =>
    set((state: GooseLocationsState) => ({ gooseLocations: [...state.gooseLocations, location] })),
  removeGooseLocation: (id: string) =>
    set((state: GooseLocationsState) => ({
      gooseLocations: state.gooseLocations.filter((location) => location.id !== id),
    })),
}));

export default useGooseLocationStore;