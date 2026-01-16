import { createSlice } from '@reduxjs/toolkit';

const initialState = {
  elements: [],
};

const graphSlice = createSlice({
  name: 'graph',
  initialState,
  reducers: {
    addElement: (state, action) => {
      state.elements.push(action.payload);
    },
    deleteElement: (state, action) => {
        console.log('Deleting element ID:', action.payload);
      state.elements = state.elements.filter((element) => element.id !== action.payload);
    },
    updateElement: (state, action) => {
      const updatedNodeIndex = state.elements.findIndex((element) => element.id === action.payload.id);
      if (updatedNodeIndex !== -1) {
        console.log('Updating element:', action.payload);
        state.elements[updatedNodeIndex] = action.payload; 
      }
    },
  },
});

export const { addElement, deleteElement, updateElement } = graphSlice.actions;

export default graphSlice.reducer;