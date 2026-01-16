import { createAsyncThunk } from '@reduxjs/toolkit';

const BASE_URL = '';

export const fetchRegisterUser = createAsyncThunk(
  'user/fetchRegisterUser',
  async (user, { rejectWithValue }) => {
    try {
      const response = await fetch(`${BASE_URL}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json;charset=utf-8',
        },
        body: JSON.stringify(user),
      });

      if (!response.ok) {
        const data = await response.json();
        return rejectWithValue(data.errors);
      }

      const data = await response.json();
      return data;
    } catch (e) {
      return rejectWithValue(e);
    }
  },
);

export const fetchLoginUser = createAsyncThunk(
  'user/fetchLoginUser',
  async (user, { rejectWithValue }) => {
    try {
      const response = await fetch(`${BASE_URL}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json;charset=utf-8',
        },
        body: JSON.stringify(user),
      });

      if (!response.ok) {
        const data = await response.json();
        return rejectWithValue(data.errors);
      }

      const data = await response.json();
      return data;
    } catch (e) {
      return rejectWithValue(e);
    }
  },
);