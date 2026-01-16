import {ActionCreator, AnyAction} from 'redux';

import {ITheme} from 'components/Styled/themes';

export enum ThemeActions {
    THEME_UPDATED = 'THEME_UPDATED'
}

export interface IThemeUpdatedAction extends AnyAction {
    payload: ITheme;
}

const updateTheme: ActionCreator<IThemeUpdatedAction> = (theme: ITheme) => {
    return {
        payload: theme,
        type: ThemeActions.THEME_UPDATED
    };
};

export default {
    updateTheme
};