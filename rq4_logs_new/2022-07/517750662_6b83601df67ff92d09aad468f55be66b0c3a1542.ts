import { createAsyncThunk } from '@reduxjs/toolkit'
import api from 'api'
import axios from 'axios'
import { path } from 'config'
import { PostType } from 'types'

export const fetchPosts = createAsyncThunk('posts/fetchPosts', async (_, thunkAPI) => {
  try {
    const response = await api.get<PostType[]>(path.posts)

    return response.data
  } catch (error) {
    if (axios.isAxiosError(error) && error.response) {
      return thunkAPI.rejectWithValue((error.response.data as Error).message)
    }

    return thunkAPI.rejectWithValue('Не удалось получить статьи')
  }
})