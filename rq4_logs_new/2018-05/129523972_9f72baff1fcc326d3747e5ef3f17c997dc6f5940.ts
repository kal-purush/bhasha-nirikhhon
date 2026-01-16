import {ActionCreator, AnyAction} from 'redux';
import {IWindowDimensions} from './windowTypes';

export enum WindowActions {
    WINDOW_DIMENSIONS_UPDATED = 'WINDOW_DIMENSIONS_UPDATED'
}

export interface IWindowDimensionsUpdatedAction extends AnyAction {
    payload: IWindowDimensions
}

const windowDimensionsUpdated: ActionCreator<IWindowDimensionsUpdatedAction> = (dimensions: IWindowDimensions) => {
    return {
        payload: dimensions,
        type: WindowActions.WINDOW_DIMENSIONS_UPDATED
    }
};

export default {
    windowDimensionsUpdated
}