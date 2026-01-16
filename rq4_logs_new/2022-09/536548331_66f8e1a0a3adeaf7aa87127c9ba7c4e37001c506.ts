import { createSlice } from "@reduxjs/toolkit";
import { TodoState } from "../types/state/todos";

let nextTodoId = 0;

const initialState: TodoState = {
  allIds: [],
  byId: {},
};

const todosSlice = createSlice({
  name: "todos",
  initialState,
  reducers: {
    addTodo(state, action) {
      const content = action.payload;
      let id = ++nextTodoId;

      state.allIds.push(id);
      state.byId[id] = {
        content,
        completed: false,
      };
    },
    toggleTodo(state, action) {
      const id = action.payload;
      const todo = state.byId[id];
      todo.completed = !todo.completed;
    },
  },
});

export const { addTodo, toggleTodo } = todosSlice.actions;
export default todosSlice.reducer;