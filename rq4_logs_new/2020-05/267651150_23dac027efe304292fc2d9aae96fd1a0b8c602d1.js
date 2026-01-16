import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { selectNumOfItems } from "../store/cart/selectors";

function Buttons(props) {
  const dispatch = useDispatch();
  const numOfItems = useSelector(selectNumOfItems(props.id));

  return (
    <div>
      <button
        className="buttons"
        /* onClick={() => dispatch(rmFromCart(props.id, props.price))} */
      >
        -
      </button>
      <span>{numOfItems} in cart</span>
      <button
        className="buttons"
        /* onClick={() => dispatch(addToCart(props.id, props.price))} */
      >
        +
      </button>
    </div>
  );
}

export default Buttons;