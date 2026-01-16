import { createSlice, createAsyncThunk } from "@reduxjs/toolkit";
import { CourseService } from "../../service/courseService"; // This service should handle your API requests
import { addToast } from "./toasterSlice"; // Optional, to show success/error toast notifications
import { Chapter } from "../../types/course.types"; // Define your Chapter type if not already defined

// Define the Course state type for adding chapters
interface ChapterState {
  chapter: Chapter | null;
  loading: boolean;
  error: string | null;
  success: boolean;
}

// Initial state
const initialState: ChapterState = {
  chapter: null,
  loading: false,
  error: null,
  success: false,
};

// Async thunk for adding a chapter to a module
export const addChapter = createAsyncThunk<
  Chapter, // Return type
  { moduleId: string; chapterData: object }, // Argument type (moduleId and chapter data)
  { rejectValue: string }
>("add-chapter/add", async ({ moduleId, chapterData }, { dispatch, rejectWithValue }) => {
  try {
    const response = await CourseService.addChapterToModule(moduleId, chapterData);

    if (response.data.success) {
      dispatch(
        addToast({
          message: "Chapter added successfully!",
          type: "success",
          duration: 3000,
          position: "top-right",
        })
      );
      return response.data.chapter; // Assuming the response contains the added chapter
    } else {
      dispatch(
        addToast({
          message: response.data.errorMessage || "Failed to add chapter.",
          type: "error",
          duration: 3000,
          position: "top-right",
        })
      );
      return rejectWithValue(response.data.errorMessage || "Failed to add chapter.");
    }
  } catch (error: any) {
    const message = error.response?.data?.errorMessage || "Failed to add chapter.";
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
});

// Define the chapter slice
const addChapterSlice = createSlice({
  name: "add-chapter",
  initialState,
  reducers: {
    // If needed, define actions here to update local state
    resetChapter: (state) => {
      state.chapter = null;
      state.error = null;
      state.success = false;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(addChapter.pending, (state) => {
        state.loading = true;
        state.error = null;
        state.success = false;
      })
      .addCase(addChapter.fulfilled, (state, action) => {
        state.chapter = action.payload;
        state.loading = false;
        state.success = true;
      })
      .addCase(addChapter.rejected, (state, action) => {
        state.error = action.payload || "Chapter addition failed";
        state.loading = false;
        state.success = false;
      });
  },
});

// Export actions
export const { resetChapter } = addChapterSlice.actions;

// Export reducer
export default addChapterSlice.reducer;