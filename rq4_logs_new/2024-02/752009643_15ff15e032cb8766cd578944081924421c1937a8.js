import { createSlice } from "@reduxjs/toolkit"

const initialState = {
    addActivity:[
        {text: "Running"},
        {text: "swimming"}
    ]
}

// Creating Reducer using Redux Toolkit
const addActivitySlice = createSlice({
    name: 'addActivity',
    initialState,
    reducers:{
        // this is an add action
        add: (state, action) => {
            state.addActivity.push({
                text:action.payload
            })
        },

        // this is a delete action
        delete: (state, action) => {
            state.addActivity.splice(action.payload,1)
        }
    }
})

// export reducer
export  const addActivityReducer = addActivitySlice.reducer