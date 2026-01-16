import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { TodoItemProps } from 'component/TodoItem';
import { TodoListProps } from 'component/TodoList';
import { RootState } from './store';

export interface TodoState {
  todoData: TodoItemProps[];
}


const initialState: TodoState = {
  todoData: [
    {
      task: 'test',
    },
  ],
};

export const todoSlice = createSlice({
  name: 'todo',
  initialState,
  reducers: {
    AddTodo: (state, action: PayloadAction<TodoItemProps>) => {
      console.log(state);
      // console.log([...state.todoData, action.payload]);
      [...state.todoData, action.payload];
    },
    DeleteTodo: (state, action: PayloadAction<TodoItemProps>) => {
      state.todoData.filter((item, index) => index !== action.payload.index);
    },
    UpdateTodo: (state, action: PayloadAction<TodoItemProps>) => {
      state.todoData.map((item, index) =>
        index === action.payload.index
          ? { ...item, task: action.payload.task }
          : item
      );
    },
  },
});

export const { AddTodo, DeleteTodo, UpdateTodo } = todoSlice.actions;
// export const selectTodo = (state: RootState) => state.todoData;
export const todoReducer = todoSlice.reducer;