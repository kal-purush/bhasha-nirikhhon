import { createAsyncThunk } from '@reduxjs/toolkit';

const BASE_URL = '';

export const fetchRegisterUser = createAsyncThunk('user/fetchRegisterUser', async function (user, { rejectWithValue }) {
    try {
        const response = await fetch(`${BASE_URL}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json;charset=utf-8',
            },
            body: JSON.stringify(user),
        });

        if (!response.ok) {
            const data = await response.json();
            return rejectWithValue(data.errors);
        }

        const data = await response.json();
        localStorage.setItem('token', data.user.token);
        return data;
    } catch (e) {
        return rejectWithValue(e);
    }
});

export const fetchLoginUser = createAsyncThunk('user/fetchLoginUser', async function (user, { rejectWithValue }) {
    try {
        const response = await fetch(`${BASE_URL}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json;charset=utf-8',
            },
            body: JSON.stringify(user),
        });

        if (!response.ok) {
            const data = await response.json();
            return rejectWithValue(data.errors);
        }

        const data = await response.json();
        localStorage.setItem('token', data.user.token);
        return data;
    } catch (e) {
        return rejectWithValue(e);
    }
});
