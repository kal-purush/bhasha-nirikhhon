import { create } from 'zustand'

interface CalendarState {
  events: Event[]
  isLoading: boolean
  addEvent: (event: Event) => void
  setLoading: (loading: boolean) => void
}

interface Event {
  id: string
  title: string
  date: Date
  description?: string
}

export const useStore = create<CalendarState>((set) => ({
  events: [],
  isLoading: false,
  addEvent: (event) => set((state) => ({ 
    events: [...state.events, event] 
  })),
  setLoading: (loading) => set({ isLoading: loading }),
})); 