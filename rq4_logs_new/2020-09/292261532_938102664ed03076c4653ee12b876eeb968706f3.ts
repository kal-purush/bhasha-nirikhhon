import * as ActionTypes from '../constants/actionTypes';

export interface PostIdea {
  content: string;
  createdAt: Date;
  updatedAt: Date;
  // TODO: before implements
  // uid: string
  // tags: []
  // favoriteCount: number
};

export interface PostIdeaState {
  isLoading: boolean;
  postIdea: PostIdea;
  postIdeas: PostIdea[];
};

export interface PostIdeaStart {
  type: typeof ActionTypes.POST_IDEA_START;
  payload: PostIdea
}

export interface PostIdeaSuccess {
  type: typeof ActionTypes.POST_IDEA_SUCCESS;
}

export interface PostIdeaFailure {
  type: typeof ActionTypes.POST_IDEA_FAILURE;
}

export type PostIdeaAction = 
  | PostIdeaStart
  | PostIdeaSuccess
  | PostIdeaFailure;