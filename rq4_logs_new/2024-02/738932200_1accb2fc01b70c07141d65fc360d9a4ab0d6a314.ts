import { takeLatest, put, call } from 'redux-saga/effects';
import axios from 'axios';
import { createUser, getUserCredentials, getUserDetails, loginFailure, loginSuccess, setUser } from '../slices/authSlice';
import { storeInSession } from '../../utility/sessionStorage';

function* setUserWorker(action:any) {
  try {
    const { data } = yield call(axios.post, 'http://localhost:4000/api/users/login', action.payload);

    yield put(loginSuccess());
    yield put(setUser(data)); 
    storeInSession('user', JSON.stringify(data));
  } catch (error:any) {
    yield put(loginFailure(error.response?.data.error || 'Login failed. Please check your credentials.'));
    console.error(error.response?.data.error);
  }
}

function* createUserWorker(action:any) {
    try {
      const { data } = yield call(axios.post, 'http://localhost:4000/api/users/register', action.payload);
  
      yield put(loginSuccess());
      yield put(createUser(data)); 
      storeInSession('user', JSON.stringify(data));
    } catch (error:any) {
      yield put(loginFailure(error.response?.data.error || 'Login failed. Please check your credentials.'));
      console.error(error.response?.data.error);
    }
  }

export function* authSaga() {
  yield takeLatest(getUserCredentials, setUserWorker);
  yield takeLatest(getUserDetails, createUserWorker);
}