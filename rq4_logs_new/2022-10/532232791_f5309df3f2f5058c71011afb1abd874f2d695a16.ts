import { ResponseNotification } from '@Models/notification'
import { PayloadAction } from '@reduxjs/toolkit'
import { all, call, put, takeLatest } from 'redux-saga/effects'
import notificationApi from '../../api/notificationApi'
import {
  deleteNotificationActions,
  editNotificationActions,
  notificationActions,
} from '../slices/notificationSlice'

function* getNotification(action: PayloadAction<string>) {
  try {
    const response: ResponseNotification = yield call(
      notificationApi.getByUser,
      action.payload
    )

    yield put(notificationActions.getNotificationSuccess(response.data))
  } catch (error) {
    console.log('Get notification by user faild', error)
    yield put(notificationActions.getNotificationFaild())
  }
}

function* deleteNotification(action: PayloadAction<String>) {
  try {
    yield call(notificationApi.deleteNotification, action.payload)
    yield put(deleteNotificationActions.deleteNotificationSuccess())
  } catch (error) {
    yield put(deleteNotificationActions.deleteNotificationFaild())
  }
}

function* editNotification(action: PayloadAction<String>) {
  try {
    yield call(notificationApi.deleteNotification, action.payload)
    yield put(editNotificationActions.editNotificationSuccess())
  } catch (error) {
    yield put(editNotificationActions.editNotificationFaild())
  }
}

export default function* notificationSaga() {
  yield all([
    takeLatest(notificationActions.getNotification.type, getNotification),
    takeLatest(
      deleteNotificationActions.deleteNotification.type,
      deleteNotification
    ),
    takeLatest(editNotificationActions.editNotification.type, editNotification),
  ])
}