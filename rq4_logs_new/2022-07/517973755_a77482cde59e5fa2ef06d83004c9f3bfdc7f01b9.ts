import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { IUser } from './user.model';

export const initialState: IUser = {
   email: '',
   name: '',
   isAuth: false,
};

const userSlice = createSlice({
   name: 'user',
   initialState,
   reducers: {
      signIn: (state, action: PayloadAction<IUser>) => {
         state.email = action.payload.email;
         state.name = action.payload.name;
         state.isAuth = true;
      },
      signOut: () => {
         return initialState;
      },
   },
});

export const { signIn, signOut } = userSlice.actions;
export default userSlice.reducer;