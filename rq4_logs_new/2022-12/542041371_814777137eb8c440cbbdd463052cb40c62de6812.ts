import { createAsyncThunk } from '@reduxjs/toolkit';
import forumService from '../../pages/forum/services/forum-service';
import { LikeDislike, User } from '../../types/forum';
import { UserDTO } from '../../types/user';

export const fetchForum = createAsyncThunk(
  'forum/fetchForum',
  async (_, thunkAPI) => {
    try {
      const { data, error } = await forumService.getForum();
      if (error) {
        return thunkAPI.rejectWithValue(error);
      }
      return data;
    } catch (error: any) {
      return thunkAPI.rejectWithValue(
        `Не удалось загрузить форум. ${error.message}`
      );
    }
  }
);

export const createThread = createAsyncThunk(
  'forum/createThread',
  async (payload: { content: string; user: UserDTO }, thunkAPI) => {
    try {
      const { data, error } = await forumService.createThread(payload);
      if (error) {
        return thunkAPI.rejectWithValue(error);
      }
      return data;
    } catch (error: any) {
      return thunkAPI.rejectWithValue(
        `Не удалось создать тред. ${error.message}`
      );
    }
  }
);

export const createPost = createAsyncThunk(
  'forum/createPost',
  async (
    payload: { content: string; threadId: number; user: UserDTO },
    thunkAPI
  ) => {
    try {
      const { data, error } = await forumService.createPost(payload);
      if (error) {
        return thunkAPI.rejectWithValue(error);
      }
      return data;
    } catch (error: any) {
      return thunkAPI.rejectWithValue(
        `Не удалось создать пост. ${error.message}`
      );
    }
  }
);

export const createComment = createAsyncThunk(
  'forum/createComment',
  async (
    payload: { content: string; postId: number; user: UserDTO },
    thunkAPI
  ) => {
    try {
      const { data, error } = await forumService.createComment(payload);
      if (error) {
        return thunkAPI.rejectWithValue(error);
      }
      return data;
    } catch (error: any) {
      return thunkAPI.rejectWithValue(
        `Не удалось создать комментарий. ${error.message}`
      );
    }
  }
);

export const setLike = createAsyncThunk(
  'forum/setLike',
  async (payload: LikeDislike & User, thunkAPI) => {
    try {
      const { data, error } = await forumService.setLike(payload);
      if (error) {
        return thunkAPI.rejectWithValue(error);
      }
      return data;
    } catch (error: any) {
      return thunkAPI.rejectWithValue(
        `Не удалось поставить лайк. ${error.message}`
      );
    }
  }
);

export const setDislike = createAsyncThunk(
  'forum/setDislike',
  async (payload: LikeDislike & User, thunkAPI) => {
    try {
      const { data, error } = await forumService.setDislike(payload);
      if (error) {
        return thunkAPI.rejectWithValue(error);
      }
      return data;
    } catch (error: any) {
      return thunkAPI.rejectWithValue(
        `Не удалось поставить дизлайк. ${error.message}`
      );
    }
  }
);

const actions = {
  fetchForum,
  createThread,
  createPost,
  createComment,
  setLike,
  setDislike,
};

export default actions;