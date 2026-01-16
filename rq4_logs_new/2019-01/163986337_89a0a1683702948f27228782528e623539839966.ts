import { RecentPostsActions, RecentPostsActionTypes } from './recentPosts.actions';
import { Post } from '../postsList/postsList.model';

export interface RecentPostsState {
  recentPosts: Post[]
}

export const initialState: RecentPostsState = {
  recentPosts: []
};

export function recentPostsReducer(state = initialState, action: RecentPostsActions): RecentPostsState {
  switch (action.type) {

    case RecentPostsActionTypes.SetRecentPosts: {
      return {
        ...state,
        recentPosts: action.recentPosts
      }
    }

    default: {
      return state;
    }
  }
}