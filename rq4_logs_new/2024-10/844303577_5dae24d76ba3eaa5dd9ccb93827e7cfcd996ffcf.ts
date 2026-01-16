import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";

import type { TList, TListSlug } from "@/types/List";
import type { TAnime } from "@/types/Anime";

type TLists = {
  lists: Partial<Record<TListSlug, TList>>;
  addList: (list: TList) => void;
  updateList: (list: TList) => void;
  removeList: (slug: TListSlug) => void;
  addAnimeToList: (slug: TListSlug, anime: TAnime) => void;
  removeAnimeFromList: (slug: TListSlug, anime: TAnime) => void;
};

const useLists = create(
  persist<TLists>(
    (set, get) => ({
      lists: {},

      // List actions
      addList(list: TList) {
        const { lists } = get();

        if (lists[list.slug]) throw new Error("List already exists");

        return set((state) => ({ ...state, lists: { ...state.lists, [list.slug]: list } }));
      },
      updateList(list: TList) {
        const { lists } = get();

        if (!lists[list.slug]) throw new Error("List does not exist");

        return set((state) => ({ ...state, lists: { ...state.lists, [list.slug]: list } }));
      },
      removeList(slug: TListSlug) {
        const { lists } = get();

        if (!lists[slug]) throw new Error("List does not exist");

        const { [slug]: _, ...rest } = lists;

        return set((state) => ({ ...state, lists: rest }));
      },

      // List anime actions
      addAnimeToList(slug: TListSlug, anime: TAnime) {
        const { lists } = get();

        if (!lists[slug]) throw new Error("List does not exist");

        return set((state) => ({
          ...state,
          lists: {
            ...state.lists,
            [slug]: {
              ...state.lists[slug],
              animes: [...state.lists[slug]!.animes, { anime, watchedAt: new Date().getTime() }],
            },
          },
        }));
      },
      removeAnimeFromList(slug: TListSlug, anime: TAnime) {
        const { lists } = get();

        if (!lists[slug]) throw new Error("List does not exist");

        return set((state) => ({
          ...state,
          lists: {
            ...state.lists,
            [slug]: {
              ...state.lists[slug],
              animes: state.lists[slug]!.animes.filter((a) => a.anime.id !== anime.id),
            },
          },
        }));
      },
    }),
    {
      name: "anime-lists",
      storage: createJSONStorage(() => localStorage),
    }
  )
);

export default useLists;