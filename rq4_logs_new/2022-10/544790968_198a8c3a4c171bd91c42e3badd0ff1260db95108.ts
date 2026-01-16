import { writable } from "svelte/store";

export const blogStore = writable({
  blogList: [],
});