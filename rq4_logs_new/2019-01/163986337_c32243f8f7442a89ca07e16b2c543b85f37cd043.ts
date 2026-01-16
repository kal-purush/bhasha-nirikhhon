import { ActionReducerMap, createFeatureSelector } from '@ngrx/store';

import * as fromPostsList from '../content/components/postsList/postsList.reducer';
import * as fromRecentPosts from '../content/components/recentPosts/recentPosts.reducer';
import * as fromSinglePost from '../content/components/singlePost/singlePost.reducer';

export const selectContent = createFeatureSelector<ContentState>('content');

export interface ContentState {
  postsList: fromPostsList.PostsListState;
  recentPosts: fromRecentPosts.RecentPostsState;
  singlePost: fromSinglePost.SinglePostState;
}

export const contentReducers: ActionReducerMap<ContentState> = {
  postsList: fromPostsList.postsListReducer,
  recentPosts: fromRecentPosts.recentPostsReducer,
  singlePost: fromSinglePost.singlePostReducer
};