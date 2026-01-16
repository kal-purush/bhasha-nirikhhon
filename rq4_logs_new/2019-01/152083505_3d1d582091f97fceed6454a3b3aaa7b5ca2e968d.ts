import {mockStore} from '../../utils/mockStore';
import * as fetchMock from 'fetch-mock';
import {extendedUrl} from '../../../api/service/MessageService';
import {BASE_API_URL} from '../../../api/constants/api';
import {createMessage, deleteMessage, downvoteMessage, updateMessage, upvoteMessage} from '../../../Messages/actionCreators/actionCreators';
import {RawDraftContentState} from 'react-draft-wysiwyg';
import {
    MESSAGE_APP_CREATE_MESSAGE_SUCCESS,
    MESSAGE_APP_CRUD_FAILURE,
    MESSAGE_APP_DELETE_MESSAGE_SUCCESS,
    MESSAGE_APP_DOWNVOTE_MESSAGE_SUCCESS,
    MESSAGE_APP_LOADING_STARTED, MESSAGE_APP_UPDATE_MESSAGE_SUCCESS,
    MESSAGE_APP_UPVOTE_MESSAGE_SUCCESS
} from '../../../Messages/constants/actionTypes';
import {IMessage} from '../../../Messages/model/IMessage';
import * as Immutable from 'immutable';

const message: RawDraftContentState = {
    blocks: [{
        key: '1',
        type: 'atomic',
        text: 'text',
        depth: 1,
        inlineStyleRanges: [{
            style: 'BOLD',
            offset: 1,
            length: 1
        }],
        entityRanges: [{
            key: 1,
            offset: 2,
            length: 3,
        }],
    }],
    entityMap: {}
};

const messageResponse: IMessage = {
        id: '12',
        channelId: '1',
        value: 'message value',
        createdAt: '2017',
        createdBy: 'adam',
        updatedAt: null,
        updatedBy: null,
        customData: {
            votes: 0,
        }
};

const upvotedMessage = {...messageResponse, customData: {votes: 1}};
const downvotedMessage = {...messageResponse, customData: {votes: -1}};

const initialState = {
    channelList: {
        selectedChannelId: 1
    },
    messageApp: {
        messages: {
    byId: Immutable.Map({
        12: messageResponse
    })}}
};
describe('create message action', () => {
    afterEach(() => {
        fetchMock.restore();
    });

    it('should invoke success when creating was successful', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/' + 'message', {
            status: 200,
            body: messageResponse,
        });
        const store = mockStore(initialState);
        await store.dispatch(createMessage(message));

        const actions = store.getActions();
        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({
            type: MESSAGE_APP_CREATE_MESSAGE_SUCCESS,
            payload: {
                message: {...messageResponse, channelId: initialState.channelList.selectedChannelId},
            }
        });
    });
});

describe('delete message action', () => {
    afterEach(() => {
        fetchMock.restore();
    });

    it('should dispatch crud failure when api call fails', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/123', {status: 400});
        const store = mockStore(initialState);
        await store.dispatch(deleteMessage('123'));
        const actions = store.getActions();
        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_CRUD_FAILURE, payload: {message: 'Message could not be deleted.'}});
    });

    it('should dispatch success when api call is successful', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/123', {status: 204});
        const store = mockStore(initialState);
        await store.dispatch(deleteMessage('123'));
        const actions = store.getActions();
        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_DELETE_MESSAGE_SUCCESS, payload: {messageId: '123'}});
    });
});

describe('upvote message action', () => {
    afterEach(() => {
        fetchMock.restore();
    });

    it('should dispatch failure when api call fails', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/123', {status: 400});
        const store = mockStore();
        await store.dispatch(upvoteMessage('12'));

        const actions = store.getActions();
        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_CRUD_FAILURE, payload: {message: 'Message could not be upvoted.'}});
    });

    it('should dispatch success when API call is successful', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/12', {status: 200, body: upvotedMessage});
        const store = mockStore(initialState);
        await store.dispatch(upvoteMessage('12'));

        const actions = store.getActions();

        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_UPVOTE_MESSAGE_SUCCESS,
            payload: {message: {...upvotedMessage, channelId: initialState.channelList.selectedChannelId}}
        });
    });
});


describe('downvote message action', () => {
    afterEach(() => {
        fetchMock.restore();
    });

    it('should dispatch failure when api call fails', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/123', {status: 400});
        const store = mockStore();
        await store.dispatch(downvoteMessage('12'));

        const actions = store.getActions();
        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_CRUD_FAILURE, payload: {message: 'Message could not be downvoted.'}});
    });

    it('should dispatch success when API call is successful', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/12', {status: 200, body: downvotedMessage});
        const store = mockStore(initialState);
        await store.dispatch(downvoteMessage('12'));

        const actions = store.getActions();

        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_DOWNVOTE_MESSAGE_SUCCESS,
            payload: {message: {...downvotedMessage, channelId: initialState.channelList.selectedChannelId}}
        });
    });
});


describe('edit message action', () => {
    afterEach(() => {
        fetchMock.restore();
    });

    it('should dispatch failure when api call fails', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/123', {status: 400});
        const store = mockStore();
        await store.dispatch(updateMessage('12', message));

        const actions = store.getActions();
        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_CRUD_FAILURE, payload: {message: 'Message could not be updated.'}});
    });

    it('should dispatch success when API call is successful', async () => {
        fetchMock.mock(BASE_API_URL + extendedUrl + initialState.channelList.selectedChannelId + '/message/12', {status: 200, body: downvotedMessage});
        const store = mockStore(initialState);
        await store.dispatch(updateMessage('12', message));

        const actions = store.getActions();

        expect(actions[0]).toEqual({type: MESSAGE_APP_LOADING_STARTED});
        expect(actions[1]).toEqual({type: MESSAGE_APP_UPDATE_MESSAGE_SUCCESS,
            payload: {message: {...downvotedMessage, channelId: initialState.channelList.selectedChannelId}}
        });
    });
});

