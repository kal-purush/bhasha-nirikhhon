import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import axios from 'axios';
import { OnboardingApplication } from './hrTypes';

type OnboardingApplicationsState = {
  applications: { [key: string]: OnboardingApplication };
  error: string | null;
};

export const fetchOnboardingApplications = createAsyncThunk(
  'hr/fetchOnboardingApplications',
  async (_, { rejectWithValue }) => {
    try {
      const response = await axios.get<OnboardingApplication[]>(
        'http://localhost:8000/hr/onboarding-applications'
      );

      const transformedData = response.data.reduce<{ [key: string]: OnboardingApplication }>(
        (acc, application) => {
          acc[application._id] = application;
          return acc;
        },
        {}
      );

      return transformedData;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        return rejectWithValue(error.message);
      } else {
        return rejectWithValue('An unknown error occurred');
      }
    }
  }
);

// Initial state
const initialState: OnboardingApplicationsState = {
  applications: {},
  error: null,
};

// Create slice
const onboardingSlice = createSlice({
  name: 'onboarding',
  initialState,
  reducers: {
  },
  extraReducers: (builder) => {
    builder.addCase(fetchOnboardingApplications.fulfilled, (state, action) => {
      state.applications = action.payload;
      state.error = null;
    });
    builder.addCase(fetchOnboardingApplications.rejected, (state, action) => {
      state.error = action.error.message || 'Failed to fetch onboarding applications';
    });
  },
});

export default onboardingSlice.reducer;