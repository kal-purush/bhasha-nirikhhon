import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit'; 
import { addToast } from './toasterSlice';
import { CourseService } from '../../service/courseService';
import { Course } from '../../types/course.types';

// State interface
interface CourseState {
  loading: boolean;
  instructorCourse: Course | null;
  error: string | null;
}

// Initial state
const initialState: CourseState = {
  loading: false,
  instructorCourse: null,
  error: null,
};

// ✅ Fetch Course by ID
export const fetchInstructoryCourseById = createAsyncThunk<
  Course, // return type
  string, // courseId as argument
  { rejectValue: string }
>('course/fetchInstructoryCourseById', async (courseId, { rejectWithValue, dispatch }) => {
  try {
    const response = await CourseService.getInstructorCourseById(courseId);
    return response.data.course;
  } catch (error: any) {
    dispatch(
      addToast({
        message: error.response?.data?.reason || 'Failed to fetch course',
        type: 'error',
        duration: 3000,
        position: 'top-right',
      })
    );
    return rejectWithValue(error.message);
  }
});

const courseSlice = createSlice({
  name: 'course',
  initialState,
  reducers: {
    resetCourseState: (state) => {
      state.instructorCourse = null;
      state.loading = false;
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchInstructoryCourseById.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(fetchInstructoryCourseById.fulfilled, (state, action: PayloadAction<Course>) => {
        state.loading = false;
        state.instructorCourse = action.payload;
      })
      .addCase(fetchInstructoryCourseById.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload || 'Failed to fetch course';
      });
  },
});

// Export actions
export const { resetCourseState } = courseSlice.actions;

// Export reducer
export default courseSlice.reducer;