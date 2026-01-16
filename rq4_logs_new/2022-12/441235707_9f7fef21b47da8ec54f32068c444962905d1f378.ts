import { createSlice } from "@reduxjs/toolkit";

export const draggedStoneSlice = createSlice({
  name: "draggedStone",
  initialState: {
    draggedStone: {},
  },
  reducers: {
    saveDraggedStone: (state, action) =>
      void (state.draggedStone = action.payload),
  },
});

export const { saveDraggedStone } = draggedStoneSlice.actions;

export const selectDraggedStone = (state: { draggedStone: any }) =>
  state.draggedStone;

export default draggedStoneSlice.reducer;