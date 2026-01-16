import { fork, put, take, call } from "redux-saga/effects";


import {  loadProjectSuccess } from 'actions/actionCreators';


async function loadProject2() {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    return 1234;
}


import { loadProject } from 'reducers/json';


function* handleRequestUser() {
    while (true) {
        const action = yield take('LOAD_PROJECT');

       const data =  yield call(loadProject);

       console.log(data);

        yield put(loadProjectSuccess(data)); 

        /*
        const { payload, error } = yield call(loadProject2, action.payload);
        if (payload && !error) {
            yield put(loadProjectSuccess(payload));
        } else {
            yield put(loadProjectSuccess(error));
        }
        */
    }
}


export default function* rootSaga() {
    yield fork(handleRequestUser);
};