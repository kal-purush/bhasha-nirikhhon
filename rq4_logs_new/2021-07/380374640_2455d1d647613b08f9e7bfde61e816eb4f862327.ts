import { MyIssueState } from './stateTypes';
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { addIssueToChannelApi, addIssueToUserApi } from '../../../services';
import { Issue, IssueWithChannelId, User } from '../../../types';
import _ from 'lodash';

const initialState: MyIssueState[] = [
  {
    id: '1',
    title: 'Bleeding fingernails',
  },
  {
    id: '2',
    title: 'Varicose veins',
  },
  {
    id: '3',
    title: 'Arrhythmia',
  }
];

const addIssueToChannel = createAsyncThunk<MyIssueState[], IssueWithChannelId>(
  'issues/addToChannel',
  async (issueWithChannel: IssueWithChannelId) => {
    const { channelId } = issueWithChannel;
    const issue = _.omit(issueWithChannel, ['channelId']);
    const newIssue: Issue = await addIssueToChannelApi(issue, channelId).then(res => res.json());
    const userWithIssue: User = await addIssueToUserApi(newIssue).then(res => res.json());
    return userWithIssue.issueMeta;
  }
);

const closeIssue = createAsyncThunk(
  'issues/close',
  async (issue) => {
    // Set issue status to 'Closed'
    // Remove from MyIssues on user
    // Update MyIssue state
    // Add to archive state, when written
  }
);

export const myIssuesSlice = createSlice({
  name: 'issues',
  initialState,
  reducers: {
    addIssue(state, action) {
      state.concat(action.payload);
    },
    closeIssue(state, action) {
      state.filter(issue => issue.id !== action.payload.id);
    }
  },
  extraReducers: (builder) => {
    builder
    .addCase(addIssueToChannel.fulfilled, (state, action) => {
      state.concat(action.payload);
    })
  }
})