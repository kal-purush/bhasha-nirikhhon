import axios from 'axios';
import Config from '../../../config';

// Possible states
export const ADD_CATEGORY_REQUEST = 'ADD_CATEGORY_REQUEST';
export const ADD_CATEGORY_SUCCESS = 'ADD_CATEGORY_SUCCESS';
export const ADD_CATEGORY_FAILURE = 'ADD_CATEGORY_FAILURE';

function request() {
  return {
        type: ADD_CATEGORY_REQUEST,
    }
}

function receive(categories) {
  return {
        type: ADD_CATEGORY_SUCCESS,
        categories
    }
}

function error(message) {
  return {
        type: ADD_CATEGORY_FAILURE,
        message
    }
}

export function addCategory(data, id) {
    let url = Config.BASE_URL + 'food-category/add';
    if (id) {
        url += '/' + id;
    }
    return (dispatch) => {
        dispatch(request());
        return axios({
            url: url,
            method: id ? 'put' : 'post',
            responseType: 'json',
            data: data
        }).then((xhrResponse) => {
            let response = xhrResponse.data;
            if (response.success) {
                dispatch(receive(response.data));
            } else {
                dispatch(error(response.message));
            }
        }).catch(xhrResponse => {
            let response = xhrResponse.data;
            dispatch(error(response.message));
        });
    }
}