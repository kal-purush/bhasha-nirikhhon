import { createSlice } from '@reduxjs/toolkit'
import type { PayloadAction } from '@reduxjs/toolkit'
import {NewITest} from "./tests-store";
import {testsSlice} from "./testsSlice";

export interface ITestsValuesState {
    testValues: NewITest,
}

const initialState: ITestsValuesState = {
    testValues: {
        discipline: '',
        level: '',
        subject: '',
        title: '',
        difficulty: '',
        duration: 0,
        questions: [
            {
                question: '',
                answers: [{ id: '', text: '', correct: false }],
            },
        ],
    },
}

export const testValuesSlice = createSlice({
    name: 'testValues',
    initialState,
    reducers: {
        replaceValues: (state, action: PayloadAction<NewITest>) => {
            state.testValues = action.payload
        },

    },
})

export const { replaceValues } = testValuesSlice.actions

export default testValuesSlice.reducer
