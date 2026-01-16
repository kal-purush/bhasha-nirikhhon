import Toast from 'react-native-toast-message';
import axios from 'axios';
import {orderData} from "../../Utils/mockData"

import {Api} from '../../Utils/Api'
import * as types from '../types/types'


export const setOrder = data => {
  return {
    type: types.GET_ORDER,
    data,
  };
}


export const getAllOrder = (setAnimation) => {
  return async (dispatch) => {
      setAnimation(true);
      setTimeout(() => {
          dispatch(setOrder(orderData));
          setAnimation(false);
      }, 1000);
    // setAnimation(true);
    // axios.get(`${Api}/order/findall`)
    //     .then(async (res) => {
    //       dispatch(setCategories(res?.data));
    //       setAnimation(false);
    //     })
    //     .catch((err) => {
    //       setAnimation(false);
    //         Toast.show({
    //             type: 'error',
    //             text1: err?.response?.data?.message ? err?.response?.data?.message : 'Network error'
    //         });
    //     });
}
};

