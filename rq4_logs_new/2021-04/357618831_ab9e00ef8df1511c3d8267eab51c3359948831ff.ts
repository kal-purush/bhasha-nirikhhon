import { createSlice, Dispatch } from "@reduxjs/toolkit";
import { notification } from "antd";
import { persistReducer } from "redux-persist";
import storage from "redux-persist/lib/storage";
import { DB_CONNECTION } from "../../constants/db";
import { IArticle } from "../../types";
import { DataAccess } from "../../utils/DataAccess";

const initialState: {
  articlesList: Array<{ id: string; title: string; body: any; date: string }>;
  loading: boolean;
} = {
  articlesList: [],
  loading: false,
};

const articleAccess = new DataAccess<IArticle>(DB_CONNECTION, "articles");

const getArticles = async (email: string) => {
  try {
    const data = await articleAccess.get(email);

    return data;
  } catch {
    notification["error"]({
      message: "An error occured",
    });
  }
};

const slice = createSlice({
  name: "articles",
  initialState,
  reducers: {
    showLoading(state) {
      state.loading = true;
    },
    hideLoading(state) {
      state.loading = false;
    },
    reset() {
      return initialState;
    },
  },
});

export async function addArticle(data: IArticle) {
  return (dispatch: Dispatch<any>) => {
    try {
      dispatch(slice.actions.showLoading());
      articleAccess.add(data).then((res) => {
        return res;
      });
    } catch (e) {
      dispatch(slice.actions.hideLoading());
      return null;
    }
  };
}

export function editArticle() {}

export function deleteArticle() {}

const persistConfig = {
  key: "articles",
  storage,
  keyPrefix: "redux-",
  whitelist: ["articlesList"],
};

const { reset } = slice.actions;

export { getArticles, reset as resetArticles };
export default persistReducer(persistConfig, slice.reducer);