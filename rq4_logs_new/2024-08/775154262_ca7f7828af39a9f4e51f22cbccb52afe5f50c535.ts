import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { IOrder } from "../../interfaces/IOrder";
import { addItem, deleteItem, setData, State, updateItem } from "../../../../app/actions";
  
const initialState: State<IOrder> = {
    data:[]
}
    const orderSlice = createSlice({
        name: "order",
        initialState,
        reducers: {
            deleteOrder: deleteItem,
            addOrder: addItem,
            getOrders: setData,
            updateOrder: updateItem,
        }
    });

export const { deleteOrder, addOrder, getOrders, updateOrder } = orderSlice.actions;
export default orderSlice.reducer;