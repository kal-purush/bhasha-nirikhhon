import { createAsyncThunk, createSlice, PayloadAction } from '@reduxjs/toolkit';
import User from '../../helpers/responseInterfaces/User';
import {logIn, loginPayload, register} from '../../helpers/API/UsersAndSecurity';

enum scopes {
    Project,
    HugeTask,
    ToDo
}

interface LoggedState {
    logged: User|null,
    token: string,
    scope: scopes,
    errorMsg: string|undefined;
    loading: 'idle' | 'pending' | 'succeeded' | 'failed'
};

const initialState: LoggedState = {
    logged: null,
    token: "",
    scope: scopes.Project,
    errorMsg: "",
    loading: "idle"
};

const tryLog = createAsyncThunk(
    'login',
    async (payload: loginPayload) => {
      const response = await logIn(payload);
      return await response.json() as User;
    }
  )

const tryRegister = createAsyncThunk(
    'register',
    async (user: User) => {
      const response = await register(user);
      return await response.json();
    }
  )

export const loggedUserSlice = createSlice({
    name: 'loggedUser',
    initialState,
    reducers: {
      setScope: (state, action:PayloadAction<scopes>) => {
        state.scope = action.payload;
      }

      
    },
    extraReducers: (builder) => {
        builder.addCase(tryLog.pending, (state) => {
            state.loading = 'pending';
        })
        .addCase(tryRegister.pending, (state) => {
            state.loading = 'pending'
        })
        .addCase(tryRegister.rejected, (state, payload) => {
            state.loading = 'failed';
            state.errorMsg = payload.error.message;

        })
        .addCase(tryLog.rejected, (state, payload) => {
            state.loading = 'failed';
            state.errorMsg = payload.error.message;
        })
        .addCase(tryRegister.fulfilled, (state) => {
            state.loading = 'succeeded';
        })
        .addCase(tryLog.fulfilled, (state, payload) => {
            state.logged = payload.payload;
            state.loading = 'succeeded';
        })
    }
});

export const {setScope} = loggedUserSlice.actions;
export {tryLog, tryRegister, scopes};
export default loggedUserSlice.reducer;