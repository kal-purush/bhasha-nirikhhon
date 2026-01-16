import { Action } from '@ngrx/store';
import { Tweet } from '../tweet/tweet.model';

export const ActionTypes = {
    LOAD_TWEETS: 'LOAD_TWEETS',
    LOAD_TWEETS_SUCCESS: 'LOAD_TWEETS_SUCCESS',
    ADD_TWEET: 'ADD_TWEET',
    ADD_TWEET_SUCCESS: 'ADD_TWEET_SUCCESS',
    LIKE_TWEET: 'LIKE_TWEET',
    LIKE_TWEET_SUCCESS: 'LIKE_TWEET_SUCCESS'
};

export class LoadTweetsAction implements Action {
    type = ActionTypes.LOAD_TWEETS;

    constructor(public payload?: any) {
    }
}

export class LoadTweetsSuccessAction implements Action {
    type = ActionTypes.LOAD_TWEETS_SUCCESS;

    constructor(public payload: Tweet[]) {
    }
}

export class AddTweetSuccessAction implements Action {
    type = ActionTypes.ADD_TWEET_SUCCESS;

    constructor(public payload: Tweet) {
    }
}

export class AddTweetAction implements Action {
    type = ActionTypes.ADD_TWEET;

    constructor(public payload: string) {
    }
}

export class LikeTweetAction implements Action {
    type = ActionTypes.LIKE_TWEET;

    constructor(public payload: Tweet) {
    }
}

export class LikeTweetSuccessAction implements Action {
    type = ActionTypes.LIKE_TWEET_SUCCESS;

    constructor(public payload: Tweet) {
    }
}


export type Actions = LoadTweetsAction | LoadTweetsSuccessAction | AddTweetAction | AddTweetSuccessAction;