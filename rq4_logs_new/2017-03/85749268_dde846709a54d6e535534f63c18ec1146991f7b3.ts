import { Observable } from 'rxjs/Observable';
import { Store } from '../types';

export const appError = (error: any): Store.Action => ({
    type: 'APP_ERROR',
    payload: { error },
});

export const appOnline = (online: boolean): Store.Action => ({
    type: 'APP_ONLINE',
    payload: { online },
});

export const appShowMenu = (menuShown: boolean): Store.Action => ({
    type: 'APP_SHOW_MENU',
    payload: { menuShown },
});

export const toggleBaseline = (): Store.Action => ({
    type: 'TOGGLE_BASELINE',
});

export const setTheme = (theme: string): Store.Action => ({
    type: 'SET_THEME',
    payload: { theme },
});