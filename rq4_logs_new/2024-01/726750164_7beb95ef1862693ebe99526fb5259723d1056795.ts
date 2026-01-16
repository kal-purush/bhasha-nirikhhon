import { createSlice } from "@reduxjs/toolkit";

export interface AvatarState {
  url: string;
  alt: string;
}

const initialState: AvatarState = {
  url: "",
  alt: "Ảnh đại diện",
};

const userAvatarSlice = createSlice({
  name: "userAvatar",
  initialState,
  reducers: {
    setUserAvatar: (state, action) => {
      const { url, name } = action.payload;
      state.url = url;
      state.alt = name;
    },
  },
});

export const { setUserAvatar } = userAvatarSlice.actions;

export default userAvatarSlice.reducer;