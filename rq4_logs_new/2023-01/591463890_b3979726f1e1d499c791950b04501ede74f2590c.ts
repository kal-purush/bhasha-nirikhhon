import {createAsyncThunk} from "@reduxjs/toolkit";
import axios from "../../utils/axios";
import {environment} from "../../environment";
import {IArticle} from "../../models/article";
import sleep from "../../utils/sleep";


export const getArticles = createAsyncThunk(
    "articles/get",
    async (_, thunkApi) => {
        try {
            const { data } = await axios.get<IArticle[]>("articles");

            await sleep(200);

            return data;

        } catch (err) {
            return thunkApi.rejectWithValue("Could not fetch articles");
        }
    }
)