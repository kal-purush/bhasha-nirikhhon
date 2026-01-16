import { ApiActionKeys, ApiActionTypes } from './apiActionDefinitions';
import Person from '../../../types/Person';
import {EngangsstonadSoknadResponseType} from "../../../types/EngangsstonadSoknad";

export function getPersonSuccess(person: Person): ApiActionTypes {
    return {
        type: ApiActionKeys.GET_PERSON_SUCCESS,
        person
    }
}

export function getPersonFailed(reason: any): ApiActionTypes {
    return {
        type: ApiActionKeys.GET_PERSON_FAILED,
        reason
    }
}

export function sendSoknadSuccess(soknad: EngangsstonadSoknadResponseType): ApiActionTypes {
    return {
        type: ApiActionKeys.SEND_SOKNAD_SUCCESS,
        soknad
    }
}

export function sendSoknadFailed(reason: any): ApiActionTypes {
    return {
        type: ApiActionKeys.SEND_SOKNAD_FAILED,
        reason
    }
}