// src/store/slices/lectureUploadSlice.ts
import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { CourseService } from "../../service/courseService";
import { addToast } from "./toasterSlice";
import { Lecture } from "../../types/course.types";

interface LectureUploadState {
  loading: boolean;
  error: string | null;
  success: boolean;
  lecture: Lecture | null;
}

const initialState: LectureUploadState = {
  loading: false,
  error: null,
  success: false,
  lecture: null,
};

// Async thunk
export const uploadLectureVideo = createAsyncThunk<
  Lecture, // response: video URL
  {
    courseId: string;
    moduleId: string;
    chapterId: string;
    title: string;
    description: string;
    file: File;
  }, // input
  { rejectValue: string }
>(
  "upload-lecture/upload",
  async (
    { courseId, moduleId, chapterId, title, description, file },
    { dispatch, rejectWithValue }
  ) => {
    try {
      const response = await CourseService.uploadLectureVideo(
        courseId,
        moduleId,
        chapterId,
        title,
        description,
        file
      );

      if (response.data.success) {
        dispatch(
          addToast({
            message: "Lecture video uploaded successfully!",
            type: "success",
            duration: 3000,
            position: "top-right",
          })
        );
        return response.data.lecture;
      } else {
        const message = response.data.reason || "Failed to upload video.";
        dispatch(
          addToast({
            message,
            type: "error",
            duration: 3000,
            position: "top-right",
          })
        );
        return rejectWithValue(message);
      }
    } catch (error: any) {
      const message = error.response?.data?.reason || "Video upload failed.";
      dispatch(
        addToast({
          message,
          type: "error",
          duration: 3000,
          position: "top-right",
        })
      );
      return rejectWithValue(message);
    }
  }
);

// Slice
const lectureUploadSlice = createSlice({
  name: "upload-lecture",
  initialState,
  reducers: {
    resetLectureUpload: (state) => {
      state.loading = false;
      state.error = null;
      state.success = false;
      state.lecture = null;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(uploadLectureVideo.pending, (state) => {
        state.loading = true;
        state.error = null;
        state.success = false;
      })
      .addCase(uploadLectureVideo.fulfilled, (state, action) => {
        state.loading = false;
        state.success = true;
        state.lecture = action.payload;
      })
      .addCase(uploadLectureVideo.rejected, (state, action) => {
        state.loading = false;
        state.success = false;
        state.error = action.payload || "Video upload failed.";
      });
  },
});

export const { resetLectureUpload } = lectureUploadSlice.actions;
export default lectureUploadSlice.reducer;