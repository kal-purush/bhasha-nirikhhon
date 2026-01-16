import { defineStore } from "pinia";
import { Isongs } from "../types";
import useToast from "./useToast";
export const useStore = defineStore("song-store", {
  state: () => ({
    users: [] as Isongs[],
    allSongs: [],
  }),
  actions: {
    //create a new book
    async create(song: Isongs) {
      let songUrl = "/api/s1/addSongs";
      const res = await fetch(songUrl, {
        method: "POST",
        headers: { "Content-type": "application/json" },
        body: song,
      });
      if (res.ok) {
        const data = await res.json();
        useToast().success(data.message);
        setTimeout(() => {
          navigateTo("/Songs");
        }, 8000);
      } else {
        const data = await res.json();
        useToast().error(data.message);
      }
      /*  .catch((err) => {
          useToast().error(err.data.message);
        })
        .then(async (res) => {
          let msg = await res;
          if (msg) return useToast().success(msg);
          useToast().error({ msg: res });
        }); */
    },
    //get all songs
  },
});