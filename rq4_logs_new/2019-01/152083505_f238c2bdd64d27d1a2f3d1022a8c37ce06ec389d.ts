import {crudResponse} from '../../../Messages/reducers/crudResponse';
import {
    CHANNEL_APP_SELECT_CHANNEL_SUCCESS
} from '../../../Channels/constants/actionTypes';
import {
    MESSAGE_APP_CREATE_MESSAGE_SUCCESS,
    MESSAGE_APP_CRUD_FAILURE,
    MESSAGE_APP_DELETE_MESSAGE_SUCCESS,
    MESSAGE_APP_DOWNVOTE_MESSAGE_SUCCESS,
    MESSAGE_APP_UPDATE_MESSAGE_SUCCESS,
    MESSAGE_APP_UPVOTE_MESSAGE_SUCCESS
} from '../../../Messages/constants/actionTypes';

describe('crud response reducer', () => {
    it('test create message failure', () => {
        expect(crudResponse(undefined, {
            type: MESSAGE_APP_CRUD_FAILURE,
            payload: { message: 'could not create message' }
        })).toEqual('could not create message');
    });

    const actionsThatShouldReturnUndefined = [
        MESSAGE_APP_CREATE_MESSAGE_SUCCESS,
        MESSAGE_APP_UPDATE_MESSAGE_SUCCESS,
        MESSAGE_APP_UPVOTE_MESSAGE_SUCCESS,
        MESSAGE_APP_DOWNVOTE_MESSAGE_SUCCESS,
        MESSAGE_APP_DELETE_MESSAGE_SUCCESS,
        CHANNEL_APP_SELECT_CHANNEL_SUCCESS,
    ];

    actionsThatShouldReturnUndefined.map((action: string) => {
        it('test ' + action + ' returning undefined', () => {
            expect(crudResponse('could not create message', {
                type: action,
            })).toEqual(undefined);
        });
    });


});