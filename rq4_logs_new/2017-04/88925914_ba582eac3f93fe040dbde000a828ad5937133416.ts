import * as _ from 'lodash';
import {ThunkAction} from 'redux-thunk';

import * as models from '../models'

import {meetups} from '../mockdata';

export interface MeetupsState {
    meetups: models.Meetup[],
    page: number,
    filter: string,
}

interface NEW_MEETUP {
    type: "NEW_MEETUP";
    meetup : models.Meetup;
}
export function createMeetup(meetup: models.Meetup): ThunkAction<Promise<NEW_MEETUP>, MeetupsState, void> {
    return (dispatch) => {
        return new Promise((resolve, reject) => {
            resolve(dispatch({
                type: "NEW_MEETUP",
                meetup
            }));
        }).then(() => {
            //TODO: not sure if I want to do this?
            return dispatch(getMeetups());
        });
    }
}

interface GET_MEETUPS {
    type: "GET_MEETUPS",
    meetups: models.Meetup[],
}
export function getMeetups(): ThunkAction<Promise<{}>, MeetupsState, void> {
    return (dispatch) => {
        return new Promise((resolve, reject) => {
            return resolve(dispatch({
                type: "GET_MEETUPS",
                meetups,
            }));
        });
    }
}

interface ADD_PRESENTER {
    type: "ADD_PRESENTER",
    meetupId: models.Meetup["id"],
    presenter: models.User,
}
export function addPresenter(meetupId: ADD_PRESENTER["meetupId"], presenter: ADD_PRESENTER["presenter"]): ThunkAction<Promise<{}>, MeetupsState, void> {
    return (dispatch) => {
        return new Promise((resolve, reject) => {
            return resolve(dispatch({
                type: "ADD_PRESENTER",
                meetupId,
                presenter
            }));
        });
    }
}

interface ADD_ATTENDEE {
    type: "ADD_ATTENDEE",
    meetupId: models.Meetup["id"],
    attendee: models.User,
}
export function addAttendee(meetupId: ADD_ATTENDEE["meetupId"], attendee: ADD_ATTENDEE["attendee"]): ThunkAction<Promise<{}>, MeetupsState, void> {
    return (dispatch) => {
        return new Promise((resolve, reject) => {
            return resolve(dispatch({
                type: "ADD_ATTENDEE",
                meetupId,
                attendee
            }));
        });
    }
}

interface ADD_CHAT_MESSAGE {
    type: "ADD_CHAT_MESSAGE",
    meetupId: models.Meetup["id"],
    message: models.Message,
}
export function addChatMessage(meetupId: ADD_CHAT_MESSAGE["meetupId"], message: ADD_CHAT_MESSAGE["message"]): ThunkAction<Promise<{}>, MeetupsState, void> {
    return (dispatch) => {
        return new Promise((resolve, reject) => {
            return resolve(dispatch({
                type: "ADD_CHAT_MESSAGE",
                meetupId,
                message,                
            }))
        });
    }
}

export type MEETUP_ACTIONS = NEW_MEETUP | GET_MEETUPS | ADD_PRESENTER | ADD_ATTENDEE | ADD_CHAT_MESSAGE;

export function meetupReducer(state: MeetupsState = {
    page: 0,
    filter : "",
    meetups,
}, action: MEETUP_ACTIONS): MeetupsState {
    switch (action.type) {
        case "NEW_MEETUP" :
            return _.merge({}, state, {
                meetups : state.meetups.concat(action.meetup)
            });
        case "GET_MEETUPS" :
            return _.merge({}, state, {
                meetups : action.meetups,
            });
        case "ADD_PRESENTER" :
            const nextState = _.cloneDeep(state);
            
            return nextState;
        case "ADD_ATTENDEE" :
        case "ADD_CHAT_MESSAGE" :
        default: 
            return state;
    }
};