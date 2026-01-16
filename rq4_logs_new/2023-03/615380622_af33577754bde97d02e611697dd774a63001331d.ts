import { createAction, createSlice } from "@reduxjs/toolkit";
import { RootState } from "../store/store";

export type PhotoStore = {
  photos: Record<string, any>[];
  totalNoOfPhotos: number;
  rowsPerPage: number;
  page: number;
};

const initialState: PhotoStore = {
  photos: [],
  totalNoOfPhotos: 0,
  rowsPerPage: 10,
  page: 0,
};

export const photoSlice = createSlice({
  name: "photo",
  initialState,
  reducers: {
    changePaginate: (state, action) => {
      const { rowsPerPage, page } = action.payload;
      return {
        ...state,
        page: page ?? state.page,
        rowsPerPage: rowsPerPage ?? state.rowsPerPage,
      };
    },
    getAllPhotosSuccess: (state, action) => {
      return {
        ...state,
        photos: action.payload.data,
        totalNoOfPhotos: (action.payload?.totalCount || [])[0]?.count ?? 0,
      };
    },
    getAllPhotosFailure: (state, action) => {
      return state;
    },
  },
});
export default photoSlice.reducer;

export const photoDataSelector = (state: RootState): PhotoStore => state.photo;

export const { getAllPhotosSuccess, getAllPhotosFailure, changePaginate } =
  photoSlice.actions;

export const getAllPhotos =
  createAction<Record<string, any>>(`Photo/GET_ALL_PhotoS`);