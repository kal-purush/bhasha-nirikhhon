import { create } from "zustand"
import { persist } from 'zustand/middleware'

interface UserStore {
  likes: string[]
  addLike: (vehicle: string) => void
  removeLike: (vehicle: string) => void

  books: string[]
  addBook: (vehicle: string) => void
  removeBook: (vehicle: string) => void
}

export const useUser = create<UserStore>()(persist((set) => ({
  likes: [],
  books: [],

  addLike: (vehicle: string) => set((state) => ({
    likes: [...state.likes, vehicle]
  })),

  addBook: (vehicle: string) => set((state) => ({
    books: [...state.books, vehicle]
  })),

  removeLike: (vehicle: string) => set((state) => ({
    likes: state.likes.filter((item) => item !== vehicle)
  })),

  removeBook: (vehicle: string) => set((state) => ({
    books: state.books.filter((item) => item !== vehicle)
  }))
}), {
  name: 'user store'
}))