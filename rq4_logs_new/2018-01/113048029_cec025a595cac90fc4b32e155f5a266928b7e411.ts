import { Observable } from 'rxjs/Observable'
import { combineEpics } from 'redux-observable';
import * as fetch from '../service/fetch';

import { LOADING_START, LOADING_ERROR, LOADING_SUCCESS } from '../constants/loading';
import { documentList } from '../service/api'

import { ERROR_DOCUMENT, FETCH_DOCUMENT, RECEIVE_DOCUMENT } from '../constants/document';

import { errorDocument } from '../actions/index';
export const loadingStart = () => ({type: LOADING_START});
export const loadingError = () => ({type: LOADING_ERROR});
export const loadingSuccess = () => ({type: LOADING_SUCCESS});

export const documentReceive = (data:any) => ({type: RECEIVE_DOCUMENT, data: data});

export const fetchAllDocument = (action$:any) =>
action$.ofType(FETCH_DOCUMENT)
  .mergeMap((action: any) => {
    return fetch.post(documentList,action.data)
    .map((response: any) => {
      console.log(response);
      if(response.state.code === 1){
        let temp = response.data;
        return documentReceive(temp);
      }else{
        return errorDocument(response.state.msg);
      }
    })
    // 只有服务器崩溃才捕捉错误
    .catch((e: any): any => {
      console.log(e)
      return Observable.of(({ type: ERROR_DOCUMENT })).startWith(loadingError())
    }).startWith(loadingSuccess()).delay(200).startWith(loadingStart())

  });

  export default combineEpics(fetchAllDocument);