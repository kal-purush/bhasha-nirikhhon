import { cartResponse } from '@Models/cart'
import { PayloadAction } from '@reduxjs/toolkit'
import { all, call, put, takeLatest } from 'redux-saga/effects'
import cartApi from '../../api/cartApi'
import { AddToCart, cartActions } from '../slices/cartSlice'

function* getCartByUser(action: PayloadAction<string>) {
  try {
    const response: cartResponse = yield call(cartApi.getCart, action.payload)

    yield put(cartActions.getCartByUserSuccess(response))
  } catch (error) {
    console.log('Get cart by user faild', error)
    yield put(cartActions.getCartByUserFaild())
  }
}

function* addToCart(action: PayloadAction<AddToCart>) {
  try {
    const response: AddToCart = yield call(cartApi.addToCart, action.payload)
    yield put(cartActions.addToCartSuccess(response))
  } catch (error) {
    yield put(cartActions.addToCartFaild())
  }
}
export default function* brandSaga() {
  yield all([
    takeLatest(cartActions.getCartByUser.type, getCartByUser),
    takeLatest(cartActions.addToCart.type, addToCart),
  ])
}