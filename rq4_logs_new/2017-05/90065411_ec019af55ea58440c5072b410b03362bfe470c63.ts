import {OtherAction} from '../../types'

// Start getting pages information
export const FETCH = 'pages/FETCH'
interface Fetch {
    type: 'pages/FETCH',
}

// Got pages information
export const FETCH_SUCCESS = 'pages/FETCH_SUCCESS'
interface FetchSuccess {
    type: 'pages/FETCH_SUCCESS',
    data: object[],
}

// Failed to get server information
export const FETCH_FAILURE = 'pages/FETCH_FAILURE'
interface FetchFailure {
    type: 'pages/FETCH_FAILURE',
}

// All server actions
export type PagesAction =
    Fetch |
    FetchSuccess |
    FetchFailure |
    OtherAction