import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import { TaskType } from '../../lib/types/modelType'

/** タスクに関するfeatureの型 */
export type TasksType = {
  tasks: TaskType[]
  currentTask: TaskType
  length: number
}

/** タスクに関するfeatureの初期値 */
const initialState: TasksType = {
  tasks: [
    {
      _id: '',
      uid: '',
      title: '',
    },
  ],
  currentTask: {
    _id: '',
    uid: '',
    title: '',
  },
  length: 0,
}

/** タスクのfeatureの初期値 */
export const taskSlice = createSlice({
  name: 'tasks',
  initialState,
  reducers: {
    setTasks: (state, action: PayloadAction<TaskType[]>) => {
      state.tasks = action.payload
      state.length = action.payload.length
    },
    addTask: (state, action: PayloadAction<TaskType>) => {
      state.tasks.push(action.payload)
      state.length++
    },
    updateTask: (state, action: PayloadAction<TaskType>) => {
      const taskIndex = state.tasks.findIndex((task) => {
        return task._id === action.payload._id
      })
      state.tasks[taskIndex] = action.payload
    },
    deleteTask: (state, action: PayloadAction<string>) => {
      const taskIndex = state.tasks.findIndex((task) => {
        return task._id === action.payload
      })
      state.tasks.splice(taskIndex, 1)
    },
    setCurrentTask: (state, action: PayloadAction<TaskType>) => {
      state.currentTask = action.payload
    },
  },
})

const { actions, reducer } = taskSlice
export const { setTasks, addTask, updateTask, deleteTask, setCurrentTask } =
  actions
/** 「タスク」に関するデータ */
export const tasksReducer = reducer