import { create } from "zustand";
import { IMangaData } from "../models/manga";
import { persist, createJSONStorage } from "zustand/middleware";
import { IMangaStore } from "../models/favoriteManga";

export const useMangaFavoriteStore = create<IMangaStore>()(
  persist(
    (set, get) => ({
      favoriteMangas: [],
      addFavoriteManga: (mangaData: IMangaData) =>
        set({
          favoriteMangas: get().favoriteMangas.includes(mangaData)
            ? get().favoriteMangas
            : [...get().favoriteMangas, mangaData],
        }),
      removeFavoriteManga: (mangaData: IMangaData) =>
        set({
          favoriteMangas: get().favoriteMangas.filter(
            (favorite: IMangaData) => favorite !== mangaData
          ),
        }),
    }),
    {
      name: "favoriteMangaStorage",
      storage: createJSONStorage(() => localStorage),
    }
  )
);