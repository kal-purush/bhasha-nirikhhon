import { types } from "../types/types";

export const setError = ( error ) => ({
    type: types.uiSetError,
    payload: error
})

export const RemoveError = ( error ) => ({
    type: types.uiRemoveError
})