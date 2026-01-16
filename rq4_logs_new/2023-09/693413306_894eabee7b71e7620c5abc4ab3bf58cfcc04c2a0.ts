import {createSlice} from '@reduxjs/toolkit';
import type {PayloadAction} from '@reduxjs/toolkit'
import { Task, User } from '@/utils/types';

const placeholderImg: string =
    'https://www.dovercourt.org/wp-content/uploads/2019/11/610-6104451_image-placeholder-png-user-profile-placeholder-image-png-286x300.jpg';


const initialState: Task[] = [
  {
    id: 230,
    title: 'Tìm hiểu cơ bản về NextJS, Typescript',
    createdDate: new Date('9/18/2023'),
    deadline: new Date('9/19/2023'),
    creator: { id: 100, name: 'Abigail', image: placeholderImg },
    assignees: [
      { id: 101, name: 'Alexandra', image: placeholderImg },
      { id: 102, name: 'Alison', image: placeholderImg },
    ],
    completed: false,
  },
  {
    id: 231,
    title: 'Tạo source todo app build từ NextJs 13 App router',
    createdDate: new Date('9/18/2023'),
    deadline: new Date('9/19/2023'),
    creator: { id: 100, name: 'Abigail', image: placeholderImg },
    assignees: [
      { id: 101, name: 'Alexandra', image: placeholderImg },
      { id: 102, name: 'Alison', image: placeholderImg },
    ],
    completed: false,
  },
  {
    id: 232,
    title: `Tạo trang danh sách todo với các trường tên, ngày tạo, ngày deadline, trạng thái, người tạo, người được assign`,
    createdDate: new Date('9/18/2023'),
    deadline: new Date('9/19/2023'),
    creator: { id: 100, name: 'Abigail', image: placeholderImg },
    assignees: [
      { id: 101, name: 'Alexandra', image: placeholderImg },
      { id: 102, name: 'Alison', image: placeholderImg },
    ],
    completed: false,
  },
];

const tasksSlice = createSlice({
  name: 'tasks',
  initialState,
  reducers: {
    createTask: (state, action:PayloadAction<{title:string, deadline: Date, assignees: User[]}>) => {
      const data = {
        id: Math.random() * Math.pow(10, 12),
        createdDate: new Date(),
        creator: {
          id: 3,
          name: 'me',
          image: placeholderImg
        },
        completed: false,
        ...action.payload
      };
      state.push({...data})
    },
    removeTask: (state, action:PayloadAction<{id: number}>) => {
      const idx = state.findIndex(task => task.id === action.payload.id);
      if(idx !== -1) state.splice(idx, 1);
    },
    changeState: (state, action: PayloadAction<{id: number}>) => {
      const idx = state.findIndex(item => item.id === action.payload.id);
      if(idx === -1) return
      state[idx].completed = !state[idx].completed;
    },
    changeTitle: (state, action: PayloadAction<{id: number, title:string}>) => {
      const idx = state.findIndex(item => item.id === action.payload.id);
      if(idx === -1) return
      state[idx].title = action.payload.title
    },
    changeAssignees: (state, action: PayloadAction<{id: number, assignees: User[]}>) => {
      const idx = state.findIndex(item => item.id === action.payload.id);
      if(idx === -1) return
      state[idx].assignees = action.payload.assignees
    },
    changeDeadline: (state, action: PayloadAction<{id: number, deadline: Date}>) => {
      const idx = state.findIndex(item => item.id === action.payload.id);
      if(idx === -1) return
      state[idx].deadline = action.payload.deadline
    }
  }
})

export const {createTask, removeTask, changeState, changeTitle, changeAssignees, changeDeadline} = tasksSlice.actions
export default tasksSlice.reducer