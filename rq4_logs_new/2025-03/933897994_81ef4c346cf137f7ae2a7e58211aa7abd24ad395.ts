import { defineStore } from "pinia";
import { AnimeState, CrunchyrollMedia } from "./types";

export const useAnimeStore = defineStore("anime", {
  state: (): AnimeState => ({
    observer: null,
    metadata: {
      series_id: "",
      title: "",
      series_title: "",
      season_title: "",
      episode_number: "",
    },
  }),
  actions: {
    crunchyrollMedia(): CrunchyrollMedia {
      const selfWindow = window as typeof window & {
        self: { v1config: { media: CrunchyrollMedia } };
      };
      return selfWindow.self.v1config.media;
    },
    initAnimeHandler() {
      this.getAnime();
      this.animeObserver();
    },
    getAnime() {
      const { metadata } = this.crunchyrollMedia();

      const animeData = {
        series_id: metadata.series_id,
        title: metadata.title,
        series_title: metadata.series_title,
        season_title: metadata.season_title,
        episode_number: metadata.episode_number,
      };

      this.metadata = animeData;
    },
    animeObserver() {
      this.observer = new MutationObserver(this.checkIfAnimeIsChanged);

      const playerElement = document.getElementById(
        "player0",
      ) as HTMLVideoElement;

      this.observer.observe(playerElement, {
        attributes: true,
      });
    },
    checkIfAnimeIsChanged(mutations: MutationRecord[]) {
      const changedVideo = "display: flex; height: 100%; width: 100%;";
      const isStyle = mutations[0].attributeName === "style";
      const isChangedVideo =
        (mutations[0].target as HTMLElement).getAttribute("style") ===
        changedVideo;

      if (isStyle && isChangedVideo) this.getAnime();
    },
  },
});