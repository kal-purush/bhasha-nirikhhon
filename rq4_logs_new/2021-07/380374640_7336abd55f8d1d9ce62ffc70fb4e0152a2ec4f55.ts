import { UserState } from './stateTypes';
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import * as issuesApi from '../../../services/issuesApi';
import { Issue, IssueWithChannelId, User } from '../../../types';
import _ from 'lodash';

const initialState: UserState = {
  id: '',
  firstName: '',
  lastName: '',
  email: '',
  license: '',
  state: '',
};

export const userSlice = createSlice({
  name: 'user',
  initialState,
  reducers: {
    addUser(state, action) {
      const addUser = action.payload;
      return state = {
        ...addUser
      }
    }
  }
});