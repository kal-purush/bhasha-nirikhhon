import  React from "react";
import AsyncStorage from "@react-native-async-storage/async-storage";

/**
 * Action creators
 */
import {
    PROFILE_SUCCESS,
    PROFILE_FAIL,
    PROFILE,
    PROFILE_URL
} from "../constants";

import API from '../api';


//============================= LOGIN =============================//

/**
 *
 * @param data
 * @returns {{data: *, type: *}}
 * @constructor
 */
export const initUpdateProfile = (data) => {

    return function (dispatch) {

        dispatch(profile());

        API.post(PROFILE_URL, data).then((response) => {
            dispatch(updateProfileResponse(response));
        }).catch((error) => {
            console.log(error);
        })
    };
};

/**
 *
 * @returns {{type: *}}
 *
 * @constructor
 */
const profile = () => {
    return {
        type: PROFILE,
    }
};

/**
 *
 * @param response
 * @returns {{data: *, type: *}}
 * @constructor
 */
export const updateProfileResponse = (response) => {
    console.log(response);
    return {
        type: response.status? PROFILE_SUCCESS: PROFILE_FAIL,
        data: response.user,
        error: response.error ? response.error: "Une erreur est survenue. Réessayez plus tard s'il vous plaît!"
    }
};