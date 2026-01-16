import { createSlice, PayloadAction } from "@reduxjs/toolkit";


export interface IntroType {
    content: string;
}

export interface ImageType extends IntroType { //상속받기
    image: string;
  }

interface IntroState {
     contents:IntroType[];
}

const initialState: IntroState = {
    contents: [],
}

export const profileSlice = createSlice({
    name: 'profile',
    initialState,
    reducers:{
        addPicture: (state, action: PayloadAction<ImageType>) => {
            state.contents = [...state.contents, action.payload];
        },
        addIntro: (state, action: PayloadAction<IntroType>) => {
            state.contents = [...state.contents, action.payload];
    },
    }
})

export const {addPicture, addIntro} = profileSlice.actions;
export default profileSlice.reducer;