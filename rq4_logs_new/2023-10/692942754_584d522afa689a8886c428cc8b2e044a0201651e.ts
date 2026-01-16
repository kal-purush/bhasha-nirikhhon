import { createSlice, PayloadAction } from "@reduxjs/toolkit";

//현재 상태 타입 정의하기
interface UserState {
    email: string|null;
    password: string | null
    isLoggedIn: boolean;
}

//초기 사용자 상태 정의
const initialState: UserState = {
    isLoggedIn: false,
    email: null,
    password: null
}

export const userSlice = createSlice({
    name: 'user',
    initialState,
    reducers: {
        login(state, action: PayloadAction<{ email: string, password: string }>) {
            state.isLoggedIn = true;
            state.email = action.payload.email;
        },
        logout(state){
            state.isLoggedIn = false;
            state.email = null;
        }
    }
});

export const {login, logout} = userSlice.actions;

export default userSlice.reducer;