import { createAction, props } from '@ngrx/store';
import { TweetFilter } from '../state/stream.state';

export const subscribeToStream = createAction('[Stream] subscribeToStream');
export const subscribed = createAction('[Stream] subscribed', props<{ subscribed: boolean; }>());
export const unsubscribeFromStream = createAction('[Stream] unsubscribeFromStream');
export const unsubscribed = createAction('[Stream] subscribed');
export const setFilter = createAction('[Stream] filter', props<{ filter: TweetFilter; }>());
export const newTweet = createAction('[Stream] newTweet',
    props<{ text: string; verified: boolean; countryCode: string; elapsedTime: number; }>());