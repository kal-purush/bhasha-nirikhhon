import React, { createContext, useState } from "react";

export const Context = createContext({
  order: [],
  updateItem: () => {},
  deleteItem: () => {},
});

export const Provider = (props) => {
  const [order, setOrder] = useState([]);
  const [keyNum, setKeyNum] = useState(0);

  const updateItem = (food, qty, topping) => {
    const key = "Order" + keyNum;
    const price = food.price * qty;

    setKeyNum(keyNum + 1);
    setOrder([...order, ([key], { food, qty, topping, price })]);
  };

  const deleteItem = (index) => {
    setOrder(order.filter((value, i) => i !== index));
  };

  const value = {
    order,
    updateItem,
    deleteItem,
  };
  return <Context.Provider value={value}>{props.children}</Context.Provider>;
};