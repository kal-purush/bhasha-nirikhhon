import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { SubTaskType, TaskType } from "../../types";

interface TaskRequired
  extends Omit<TaskType, "created_at" | "updated_at" | "id"> {}

interface TaskState {
  task: TaskRequired;
  loading: boolean;
}

const initialState: TaskState = {
  task: {
    title: "",
    description: "",
    status: "TODO",
    column_id: "",
    subtasks: [],
  },
  loading: false,
};

export const taskSlice = createSlice({
  name: "task",
  initialState,
  reducers: {
    setTask: (state, action: PayloadAction<TaskType>) => {
      state.task = action.payload;
    },

    updateTask: (
      state,
      action: PayloadAction<{ field: keyof TaskRequired; value: any }>
    ) => {
      const { field, value } = action.payload;
      state.task[field] = value;
    },

    updateSubtask: (
      state,
      action: PayloadAction<{
        id: string;
        field: keyof SubTaskType;
        value: any;
      }>
    ) => {
      const { id, field, value } = action.payload;
      const subtask = state.task.subtasks.find((subtask) => subtask.id === id);
      //fix this typescript error
      subtask[field] = value;
    },

    deleteSubtask: (state, action: PayloadAction<{ index: number }>) => {
      const { index } = action.payload;
      state.task.subtasks.splice(index, 1);
    },

    addSubtask: (state) => {
      state.task.subtasks.push({
        title: "",
        isCompleted: false,
        task_id: state.task.id,
      });
    },
  },
});

export default taskSlice.reducer;

export const { setTask, updateTask, updateSubtask, deleteSubtask, addSubtask } =
  taskSlice.actions;