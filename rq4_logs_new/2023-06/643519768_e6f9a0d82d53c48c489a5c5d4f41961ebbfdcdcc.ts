import { createAsyncThunk } from '@reduxjs/toolkit';

import { errorCatch } from '@/api/api.helper';

import { IAuthResponse, IEmailPassword } from './user.interface';
import { EnumAuthMethod } from '@/services/auth/auth.enums';
import { removeFromStorage } from '@/services/auth/auth.helper';
import AuthService from '@/services/auth/auth.service';

export const register = createAsyncThunk<IAuthResponse, IEmailPassword>(
	'auth/register',
	async (data, thunkApi) => {
		try {
			const response = await AuthService.main(EnumAuthMethod.REGISTER, data);
			return response;
		} catch (error) {
			return thunkApi.rejectWithValue(error);
		}
	},
);

export const login = createAsyncThunk<IAuthResponse, IEmailPassword>(
	'auth/login',
	async (data, thunkApi) => {
		try {
			const response = await AuthService.main(EnumAuthMethod.LOGIN, data);
			return response;
		} catch (error) {
			return thunkApi.rejectWithValue(error);
		}
	},
);

export const logout = createAsyncThunk('auth/logout', async () => {
	removeFromStorage();
});

export const checkAuth = createAsyncThunk<IAuthResponse>(
	'auth/check-auth',
	async (_, thunkApi) => {
		try {
			const response = await AuthService.getNewTokens();
			return response.data;
		} catch (error) {
			if (errorCatch(error) === 'jwt expired') {
				thunkApi.dispatch(logout());
			}

			return thunkApi.rejectWithValue(error);
		}
	},
);