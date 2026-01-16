import { createSlice } from "@reduxjs/toolkit";

const userSlice = createSlice({
    name:'user',
    initialState:{
         userData:{}
    },
    reducers:{
        addUser:(state,action)=>{
             state.userData = {...state.userData,...action.payload}
             console.log(state.userData,'state.user user added');
             
        },
        getUser:()=>{},
        updateUser:()=>{}
    }

})
export const { addUser,getUser,updateUser } = userSlice.actions
export default userSlice.reducer; 