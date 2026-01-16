import { uiActions } from "./ui-slice";
import axios from "axios";
import { cartActions } from "./cart-slice";


 export const fetchCartData = () => {
  return async(dispatch) => {
    const fetchData = async () => {
      const responce = await axios.get(
        "https://my-http-request-908a9-default-rtdb.firebaseio.com/cart.json"
      );
      
    const data = await responce.data;
    return data;
    
    };
    try {
      const cartData=await fetchData();
      console.log(cartData)
      dispatch(cartActions.replaceCart({
        items:cartData.items || [],
        totalQuantity:Number(cartData.totalQuantity)
      }))

     
    } catch (error) {
      dispatch(
        uiActions.showNotification({
          status: "error",
          title: "Error",
          message: "Sending cart data failed",
        })
      );
    }
  };
};


export function sendCartData(cart) {
  return async (dispatch) => {
    dispatch(
      uiActions.showNotification({
        status: "Pending",
        title: "Sending",
        message: "Sending Cart Data",
      })
    );
    const sendRequest = async () => {
        console.log(cart)
      await axios.put(
        "https://my-http-request-908a9-default-rtdb.firebaseio.com/cart.json",
        {
            items:cart.items,
            totalQuantity:cart.totalQuantity
        }
      );
    };
    try {
      await sendRequest();
      dispatch(
        uiActions.showNotification({
          status: "success",
          title: "Success!",
          message: "Sent cart data successfully",
        })
      );
    } catch (error) {
      dispatch(
        uiActions.showNotification({
          status: "error",
          title: "Error",
          message: "Sending cart data failed",
        })
      );
    }
  };
}