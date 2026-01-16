import {CHANNEL_APP_SELECT_CHANNEL_FAILURE, CHANNEL_APP_SELECT_CHANNEL_SUCCESS} from '../../../Channels/constants/actionTypes';
import {loadResponse} from '../../../Messages/reducers/loadResponse';
import {
    USER_APP_GET_USERS_FAILURE,
    USER_APP_GET_USERS_SUCCESS
} from '../../../Users/constants/actionTypes';

describe('load response reducer test', () => {
    it('select channel success should return undefined', () => {
        expect(loadResponse('Could not load', {
            type: CHANNEL_APP_SELECT_CHANNEL_SUCCESS,
        })).toEqual(undefined);
    });

    it('select channel failure should return message', () => {
        expect(loadResponse(undefined, {
            type: CHANNEL_APP_SELECT_CHANNEL_FAILURE,
            payload: { message: 'channel could not be selected'}
        })).toEqual('channel could not be selected');
    });

    it('get users success should return undefined', () => {
        expect(loadResponse('Could not load', {
            type: USER_APP_GET_USERS_SUCCESS,
        })).toEqual(undefined);
    });

    it('get users failure should return message', () => {
        expect(loadResponse(undefined, {
            type: USER_APP_GET_USERS_FAILURE,
            payload: { message: 'An error occurred.'}
        })).toEqual('An error occurred.');
    });
});