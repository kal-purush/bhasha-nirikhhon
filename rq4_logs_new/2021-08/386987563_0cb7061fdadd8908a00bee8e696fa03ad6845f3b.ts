import { createAsyncThunk } from '@reduxjs/toolkit';
import { ApiService } from '../../services/apiService';

const apiService = new ApiService();

export const userLogout = createAsyncThunk('user/logout', async () => {
  const response = await apiService.logout();
  return response.data.result;
});