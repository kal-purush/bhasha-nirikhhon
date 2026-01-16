import { writable } from "svelte/store";

const BoardStore = writable([
  {
    id: 1,
    title: "Board 1",
    status: ["TODO", "DOING", "DONE"],
    tasks: [
      {
        id: 1,
        title: "task 1",
        status: "TODO",
      },
      {
        id: 2,
        title: "task 2",
        status: "DOING",
      },
    ],
  },
  {
    id: 2,
    title: "Board 2",
    status: ["TODO", "DOING", "DONE"],
    tasks: [
      {
        id: 1,
        title: "task 1",
        status: "TODO",
      },
      {
        id: 2,
        title: "task 2",
        status: "DOING",
      },
      {
        id: 3,
        title: "task 3",
        status: "DOING",
      },
      {
        id: 4,
        title: "task 3",
        status: "DONE",
      },
    ],
  },
]);

export default BoardStore;