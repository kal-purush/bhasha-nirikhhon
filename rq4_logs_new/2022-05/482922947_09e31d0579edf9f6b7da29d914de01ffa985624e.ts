import { all, takeLatest, takeEvery, put,call } from "redux-saga/effects";

import { PayloadAction } from "@reduxjs/toolkit";
import { RegisterUser, setLodStatus } from "../reducers/authReducer";
import { loadPosts, addPosts } from "../reducers/postsReducer";
import { Card } from "../../common/types";

async function getCards() {//ассинхронная функция
  fetch('https://jsonplaceholder.typicode.com/posts')
  .then(response => response.json())
  .then(json => console.log(json))
}

async function* postsSagaWorker(action: PayloadAction<Card[]>) {
  // const data: any =   yield call (getCards);
  // const json: any = yield call(() => new Promise(res => res(data.json())))
  yield put(loadPosts(getCards()))
}

export default function* postWatcher() {
  yield all([takeLatest(addPosts, postsSagaWorker)]);
}