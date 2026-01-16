import {NameSpace} from '../../constants';
import {State} from '../../types/state';


export const getErrorPost = (state: State): boolean => state[NameSpace.DataSubscribe].hasErrorPost;
export const getErrorDelete = (state: State): boolean => state[NameSpace.DataSubscribe].hasErrorDelete;
export const getSignSubscrLoadDelete = (state: State): boolean => state[NameSpace.DataSubscribe].isSubscrLoadDelete;
export const getSignSubscrLoadPost = (state: State): boolean => state[NameSpace.DataSubscribe].isSubscrLoadPost;