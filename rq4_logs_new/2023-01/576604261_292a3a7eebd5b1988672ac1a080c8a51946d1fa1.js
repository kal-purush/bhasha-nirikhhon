import { createSlice, wishlistSlice } from "@reduxjs/toolkit"
import { Toast } from "react-bootstrap"
import { toast } from "react-toastify"

const initialState ={
    wishlist:localStorage.getItem("wishlist")
    ? JSON.parse(localStorage.getItem("wishlist"))
    : [],   
    items:[],
}

const wishlistSlice = createSlice({
    name:'wishlist',
    initialState,
    reducers:{
        addTowishlist:(state,action) =>{
            const find = state.wishlist.findIndex((item)=>item.id == action.payload.id)
            if(find >= 0){
                toast.error("Product is already added", {
                    position: "top-left",
                });
            }else{
                const tempProduct = { ...action.payload, quantity: 1 }
                state.cart.push(tempProduct);
                localStorage.setItem("wishlist", JSON.stringify(state.wishlist));
            }
        },
        
        removeWishlistItem: (state, action) => {
            state.cart = state.cart.filter((item) => item.id !== action.payload);
            toast.error("Product removed from cart", {
                position: "top-left",
            });
            localStorage.setItem("cart", JSON.stringify(state.cart));

        },
    }
})

export const {addTowishlist,removeWishlistItem} = wishlistSlice.reducer
export default wishlistSlice.reducer;