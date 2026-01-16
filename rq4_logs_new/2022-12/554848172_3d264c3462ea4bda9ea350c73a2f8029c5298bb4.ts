import { createSlice } from '@reduxjs/toolkit'
import type { PayloadAction } from '@reduxjs/toolkit'
import {NewITest} from "./tests-store";

export interface ITestsState {
    testsList: NewITest[],
}

const initialState: ITestsState = {
    testsList: [],
}

export const testsSlice = createSlice({
    name: 'tests',
    initialState,
    reducers: {
        replaceTests: (state, action: PayloadAction<[]>) => {
            state.testsList = action.payload
        },

    },
})

// Action creators are generated for each case reducer function
export const { replaceTests } = testsSlice.actions

export default testsSlice.reducer