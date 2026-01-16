import { create } from 'zustand';
import { GooseSighting } from '../interfaces/gooseSighting';

interface GooseSightingsState {
  gooseSightings: GooseSighting[];
  setGooseSightings: (sightings: GooseSighting[]) => void;
  addGooseSighting: (sighting: GooseSighting) => void;
  removeGooseSighting: (id: string) => void;
}

const useGooseSightingStore = create<GooseSightingsState>()((set) => ({
  gooseSightings: [],
  setGooseSightings: (sightings: GooseSighting[]) => set({ gooseSightings: sightings }),
  addGooseSighting: (sighting: GooseSighting) =>
    set((state: GooseSightingsState) => ({ gooseSightings: [...state.gooseSightings, sighting] })),
  removeGooseSighting: (id: string) =>
    set((state: GooseSightingsState) => ({
      gooseSightings: state.gooseSightings.filter((sighting) => sighting.id !== id),
    })),
}));

export default useGooseSightingStore;