import {OtherAction} from '../../types'

// Start getting menu information
export const FETCH = 'menus/FETCH'
interface Fetch {
    type: 'menus/FETCH',
    name: string,
}

// Got menu information
export const FETCH_SUCCESS = 'menus/FETCH_SUCCESS'
interface FetchSuccess {
    type: 'menus/FETCH_SUCCESS',
    data: object[],
    name: string,
}

// Failed to get menu information
export const FETCH_FAILURE = 'menus/FETCH_FAILURE'
interface FetchFailure {
    type: 'menus/FETCH_FAILURE',
    name: string,
}

// All server actions
export type MenusAction =
    Fetch |
    FetchSuccess |
    FetchFailure |
    OtherAction