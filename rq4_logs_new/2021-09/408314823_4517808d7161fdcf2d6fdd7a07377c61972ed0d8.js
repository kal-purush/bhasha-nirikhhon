import { createSlice } from '@redux/toolkit'

const initialState = {
    origin: "null",
    destination: "null",
    travelTimeInformation: "null",
}

export const navSlice = createSlice({
    name: "nav",
    initialState,
    reducer: {
        setOrigin: (state, action) => {
            state.origin = action.payload;
        },
        setDestination: (state, action) => {
            state.destination = action.payload;
        },
        setTravelTimeInformation: (state, action) => {
            state.travelTimeInformation = action.payload;
        },
    }
})

export const {setDestination, SetOrigin, SetTravelTimeInformation} = navSlice.actions;

export const setOrigin = (state) => {state.nav.origin}
export const setDestination = (state) => {state.nav.destination}
export const setTravelTimeInformation = (state) => {state.nav.travelTimeInformation}

export default navSlice.reducer;