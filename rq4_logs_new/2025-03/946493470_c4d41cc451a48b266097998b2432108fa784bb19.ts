import { createAsyncThunk } from '@reduxjs/toolkit';
import { clearConstructor } from '../slice/ConstructorSlice';
import {
  getFeedsApi,
  getIngredientsApi,
  orderBurgerApi,
  getOrdersApi
} from '@api';
import {
  registerUserApi,
  loginUserApi,
  logoutApi,
  getUserApi,
  updateUserApi,
  TLoginData,
  TRegisterData
} from '../utils/burger-api';

export const registerUser = createAsyncThunk(
  'user/register',
  async (data: TRegisterData, { rejectWithValue }) =>
    await registerUserApi(data).catch((error) => rejectWithValue(error.message))
);

export const updateUser = createAsyncThunk(
  'user/updateUser',
  async (user: Partial<TRegisterData>, { rejectWithValue }) =>
    await updateUserApi(user).catch((error) => rejectWithValue(error.message))
);

export const loginUser = createAsyncThunk(
  'user/login',
  async (data: TLoginData, { rejectWithValue }) =>
    await loginUserApi(data).catch((error) => rejectWithValue(error.message))
);

export const getUser = createAsyncThunk(
  'user/getUser',
  async (_, { rejectWithValue }) =>
    await getUserApi().catch((error) => rejectWithValue(error.message))
);

export const logoutUser = createAsyncThunk(
  'user/logout',
  async (_, { rejectWithValue }) =>
    await logoutApi().catch((error) => rejectWithValue(error.message))
);

export const fetchIngredients = createAsyncThunk(
  'ingredients/fetchIngredients',
  async (_, { rejectWithValue }) => {
    try {
      return await getIngredientsApi();
    } catch (error) {
      return rejectWithValue(error);
    }
  }
);

export const createOrder = createAsyncThunk(
  'order/createOrder',
  async (ingredients: string[], { dispatch, rejectWithValue }) => {
    try {
      const response = await orderBurgerApi(ingredients);
      dispatch(clearConstructor());
      return response;
    } catch (error) {
      return rejectWithValue('Ошибка при создании заказа.');
    }
  }
);

export const getUserOrders = createAsyncThunk(
  'orders/fetchUserOrders',
  async (_, { rejectWithValue }) => {
    try {
      const orders = await getOrdersApi();
      return orders;
    } catch (error) {
      return rejectWithValue('Ошибка при загрузке заказов.');
    }
  }
);

export const fetchFeeds = createAsyncThunk(
  'feeds/fetchFeeds',
  async (_, { rejectWithValue }) => {
    try {
      const data = await getFeedsApi();
      return data;
    } catch (error: any) {
      return rejectWithValue(error.message);
    }
  }
);