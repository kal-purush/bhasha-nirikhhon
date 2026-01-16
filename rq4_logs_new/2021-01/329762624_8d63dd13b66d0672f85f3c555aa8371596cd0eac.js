import React from "react";
import { ListGroup } from "react-bootstrap";
import { useDispatch, useSelector } from "react-redux";
import { actionCreators } from "../../store/actions";

export default function ShoppingList() {
  const dispatch = useDispatch();
  const items = useSelector(state => state.items);

  const addItemToBasket = index => {
    dispatch(actionCreators.addToBasket(index));
  };

  const removeItemFromList = index => {
    dispatch(actionCreators.removeItem(index));
  };

  return (
    <ListGroup className="m-4" variant="flush">
      {items.map((item, index) => {
        return item.inBasket ? (
          <ListGroup.Item
            key={index}
            variant="dark"
            onClick={() => removeItemFromList(index)}
          >
            {item.value}
          </ListGroup.Item>
        ) : (
          <ListGroup.Item
            key={index}
            variant="danger"
            onClick={() => addItemToBasket(index)}
          >
            {item.value}
          </ListGroup.Item>
        );
      })}
    </ListGroup>
  );
}