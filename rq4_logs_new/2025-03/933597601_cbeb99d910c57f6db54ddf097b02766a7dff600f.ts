import { create } from "zustand";

interface FilterState {
  fields: [];
  setField: (value: string[]) => void;
}

export const useFilterStore = create<FilterState>((set:any) => ({
  fields: [],
  setField: (value: string[]) =>
    set(()=> ({fields: value}))
}));
