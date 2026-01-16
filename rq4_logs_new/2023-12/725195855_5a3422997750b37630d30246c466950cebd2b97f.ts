import { createSlice } from '@reduxjs/toolkit';

export type ElementState = {
  id: number;
  content: string;
  placeholder?: string;
};

export type TableState = {
  tId: number;
  elements: ElementState[];
};

const initialState = [
  {
    tId: 1,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 1' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 2,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 2' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 3,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 3' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 4,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 4' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 5,
    elements: [
      { id: 1, content: '', placeholder: '목표 1' },
      { id: 2, content: '', placeholder: '목표 2' },
      { id: 3, content: '', placeholder: '목표 3' },
      { id: 4, content: '', placeholder: '목표 4' },
      { id: 5, content: '', placeholder: '최종 목표' },
      { id: 6, content: '', placeholder: '목표 5' },
      { id: 7, content: '', placeholder: '목표 6' },
      { id: 8, content: '', placeholder: '목표 7' },
      { id: 9, content: '', placeholder: '목표 8' },
    ],
  },
  {
    tId: 6,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 5' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 7,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 6' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 8,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 7' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
  {
    tId: 9,
    elements: [
      { id: 1, content: '' },
      { id: 2, content: '' },
      { id: 3, content: '' },
      { id: 4, content: '' },
      { id: 5, content: '', placeholder: '목표 8' },
      { id: 6, content: '' },
      { id: 7, content: '' },
      { id: 8, content: '' },
      { id: 9, content: '' },
    ],
  },
] as TableState[];

export const table = createSlice({
  name: 'table',
  initialState,
  reducers: {},
});

export default table.reducer;