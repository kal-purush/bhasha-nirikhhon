/* External dependencies */
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { RootState } from '..';
import { getRequest, postRequest } from '../../pages/api/axois-api';
import {
  Track,
  TrackById,
  TrackByName,
  Subscription,
} from '../../types/tracksTypes';

type TokenAndTrackId = {
  token: string;
  trackId: number;
};

export const getTracks: any = createAsyncThunk(
  'getSubscriptions',
  async (token: string, { rejectWithValue }) => {
    try {
      const subscriptions = await getRequest(token, 'v1/user/subscriptions');

      return await Promise.all(
        subscriptions.map(
          async ({ track }: Subscription) =>
            await getRequest(token, `v1/user/tracks/${track}/`),
        ),
      );
    } catch (error: any) {
      return rejectWithValue(error);
    }
  },
);

export const getTrackById: any = createAsyncThunk(
  'get-track-by-id',
  async ({ token, trackId }: TokenAndTrackId, { rejectWithValue }) => {
    try {
      const response = await getRequest(token, `/v1/user/tracks/${trackId}/`);

      return response.data;
    } catch (error: any) {
      return rejectWithValue(error);
    }
  },
);

export const signUpToTrack: any = createAsyncThunk(
  'sign-up-to-track',
  async ({ token, trackId }: TokenAndTrackId, { rejectWithValue }) => {
    try {
      const response = await postRequest(token, 'v1/user/subscriptions/', {
        track: trackId,
      });

      return response;
    } catch (error: any) {
      return rejectWithValue(error);
    }
  },
);

interface userTrackState {
  error?: Error;
  loading: boolean;
  track?: '';
  tracksById: TrackById;
  tracksByName: TrackByName;
}

const initialState: userTrackState = {
  loading: false,
  tracksById: {},
  tracksByName: {},
};

const userTrackSlice = createSlice({
  name: 'track-by-id',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(getTrackById.pending, (state) => {
        state.loading = true;
      })
      .addCase(getTrackById.fulfilled, (state, { payload }) => {
        state.loading = false;
        state.track = payload;
      })
      .addCase(getTrackById.rejected, (state, { payload }) => {
        state.loading = false;
        state.error = payload;
      })
      .addCase(getTracks.pending, (state) => {
        state.error = undefined;
        state.loading = true;
      })
      .addCase(getTracks.fulfilled, (state, { payload }) => {
        const tracksById: TrackById = {};
        const tracksByName: TrackByName = {};
        payload.forEach((track: Track) => {
          tracksById[track.id] = track;
          tracksByName[track.name] = track;
        });
        state.loading = false;
        state.tracksById = tracksById;
        state.tracksByName = tracksByName;
      })
      .addCase(getTracks.rejected, (state, { payload }) => {
        state.loading = false;
        state.error = payload;
      })
      .addCase(signUpToTrack.pending, (state) => {
        state.loading = true;
      })
      .addCase(signUpToTrack.fulfilled, (state) => {
        state.loading = false;
      })
      .addCase(signUpToTrack.rejected, (state, { payload }) => {
        state.loading = false;
        state.error = payload;
      });
  },
});

export const trackState = (state: RootState) => state.trackSlice;

export default userTrackSlice.reducer;