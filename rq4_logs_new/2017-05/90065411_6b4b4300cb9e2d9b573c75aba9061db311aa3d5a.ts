import {ServersData} from './model'
import {OtherAction} from '../../types'

// Start getting servers information
export const REQUEST = 'servers/REQUEST'
interface FetchServersAction {
    type: 'servers/REQUEST',
}

// Got servers information
export const REQUEST_SUCCESS = 'servers/REQUEST_SUCCESS'
interface FetchServersSuccessAction {
    type: 'servers/REQUEST_SUCCESS',
    servers: ServersData,
}

// Failed to get server information
export const REQUEST_FAILURE = 'servers/REQUEST_FAILURE'
interface FetchServersSuccessFailure {
    type: 'servers/REQUEST_FAILURE',
}

// All server actions
export type ServersAction =
    FetchServersAction |
    FetchServersSuccessAction |
    FetchServersSuccessFailure |
    OtherAction