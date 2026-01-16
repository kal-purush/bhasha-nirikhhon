import { defineStore } from "pinia";
import { v4 as uuid } from "uuid";
interface Note {
  id: string;
  createdAt: string;
  title: string;
  content: string;
}
export const useNotesStore = defineStore("storeNotes", {
  state: () => ({
    notes: [
      {
        id: uuid(),
        createdAt: "2023-02-19T10:34:31.233Z",
        title: "Test title 1",
        content:
          "Lorem Ipsum blah blah Lorem ipsum dolor sit amet, qui minim labore adipisicing minim sint cillum sint consectetur cupidatat.",
      },
      {
        id: uuid(),
        createdAt: new Date().toISOString(),
        title: "Test title 2",
        content:
          "qui QUI PASA? minim labore adipisicing minim sint cillum sint consectetur cupidatat.",
      },
    ],
  }),
  actions: {
    deleteNote(id: string) {
      this.notes = this.notes.filter((note) => {
        note.id !== id;
        console.log(note.id);
      });
    },
  },
});