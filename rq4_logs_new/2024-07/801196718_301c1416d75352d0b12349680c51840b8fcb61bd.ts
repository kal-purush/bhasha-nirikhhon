import { createAsyncThunk } from '@reduxjs/toolkit';
import {getOrderFromServer} from "../../components/utils/api";
import {setOrder} from "./reducer";

export const orderFromServer = createAsyncThunk(
  'order/getOrder',
  async (id: string, { dispatch }) => {
    await getOrderFromServer(id)
      .then((data) => {
          dispatch(setOrder(data));
      })
      .catch((err) => {
        console.log(err);
      });
  }
);