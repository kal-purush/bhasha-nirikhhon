import { createAsyncThunk } from '@reduxjs/toolkit'
import { RootState } from '../store'

interface MyData {
  email: string
  password: string
}
const proxy = 'http://localhost:5000'

export const getUserDetails = createAsyncThunk(
  'user/getUserDetails',
  async (_, { getState, rejectWithValue }: any) => {
    try {
      // get user data from store
      const { user } = getState()

      // configure authorization header with user's token
      const config = {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${user.userToken}`,
        },
      }
      const response = await fetch(`${proxy}/user/profile`, config)
      const data = await response.json()

      return data
    } catch (error: any) {
      if (error.response && error.response.data.message) {
        return rejectWithValue(error.response.data.message)
      } else {
        return rejectWithValue(error.message)
      }
    }
  }
)

export const loginUser = createAsyncThunk(
  'user/login',
  async ({ email, password }: MyData, { rejectWithValue }) => {
    try {
      // configure header's Content-Type as JSON
      const config = {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ email, password }),
      }
      const response = await fetch(`${proxy}/user/login`, config)
      const data = await response.json()
      // store user's token in local storage
      localStorage.setItem('userToken', data.userToken)
      return data
    } catch (error: any) {
      // return custom error message from API if any
      if (error.response && error.response.data.message) {
        return rejectWithValue(error.response.data.message)
      } else {
        return rejectWithValue(error.message)
      }
    }
  }
)

export const registerUser = createAsyncThunk(
  // action type string
  'user/register',
  // callback function
  async ({ email, password }: MyData, { rejectWithValue }) => {
    try {
      // configure header's Content-Type as JSON
      // make request to backend
      const requestURL = `${proxy}/user/register`

      const config = {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ email, password }),
      }

      await fetch(requestURL, config)
    } catch (error: any) {
      // return custom error message from API if any
      if (error.response && error.response.data.message) {
        return rejectWithValue(error.response.data.message)
      } else {
        return rejectWithValue(error.message)
      }
    }
  }
)