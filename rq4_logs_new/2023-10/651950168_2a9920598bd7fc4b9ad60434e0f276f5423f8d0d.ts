import cartReducer, {
  addtoCart,
  removeFromCart,
  plusItem,
  minusItem,
  clearCart,
} from "../slices/cartSlice";

const pizza = {
   id: "7",
   title: "Margarita",
   price: 450,
   imageUrl:
     "https://dodopizza.azureedge.net/static/Img/Products/Pizza/ru-RU/d48003cd-902c-420d-9f28-92d9dc5f73b4.jpg",
   type: "thin",
   size: 26,
   count: 1,
 };
describe("cartSlice", () => {
  it("should return default state", () => {
    const result = cartReducer(undefined, { type: "" });
    expect(result).toEqual({ items: [], totalPrice: 0 });
    expect(result).toMatchSnapshot();
  });
  it("should add pizza to the cart", () => {
    const action = {
      type: addtoCart.type,
      payload: pizza
    };
    const result = cartReducer({ items: [], totalPrice: 0 }, action);
    const { items, totalPrice } = result;
    expect(items[0].title).toBe("Margarita");
    expect(totalPrice).toBe(450);
    expect(result).toMatchSnapshot();
  });
  it("should remove pizza from the cart", () => {
    const action = {
      type: removeFromCart.type,
      payload: "7",
    };
    const result = cartReducer({ items: [pizza], totalPrice: 450 }, action);
    expect(result).toEqual({ items: [], totalPrice: 0 });
    expect(result).toMatchSnapshot();
  });
  it("should change count of item positively", () => {
    const action = {
      type: plusItem.type,
      payload: pizza
    };
    const result = cartReducer({ items: [pizza], totalPrice: 450 }, action);
    const { items, totalPrice } = result;
    expect(items[0].count).toBe(2);
    expect(totalPrice).toBe(900);
    expect(result).toMatchSnapshot();
  });
  it("should change count of item negatively", () => {
    const pizzaItem = {
      id: "7",
      title: "Margarita",
      price: 450,
      imageUrl:
        "https://dodopizza.azureedge.net/static/Img/Products/Pizza/ru-RU/d48003cd-902c-420d-9f28-92d9dc5f73b4.jpg",
      type: "thin",
      size: 26,
      count: 2,
    };
    const action = {
      type: minusItem.type,
      payload: pizza
    };
    const result = cartReducer({ items: [pizzaItem], totalPrice: 900 }, action);
    const { items, totalPrice } = result;
    expect(items[0].count).toBe(1);
    expect(totalPrice).toBe(450);
    expect(result).toMatchSnapshot();
  });
  it("should clear all the cart", () => {
    const pizzas = [
      {
        id: "7",
        title: "Margarita",
        price: 450,
        imageUrl:
          "https://dodopizza.azureedge.net/static/Img/Products/Pizza/ru-RU/d48003cd-902c-420d-9f28-92d9dc5f73b4.jpg",
        type: "thin",
        size: 26,
        count: 2,
      },
      {
        id: "8",
        title: "Four seasons",
        price: 395,
        imageUrl:
          "https://dodopizza.azureedge.net/static/Img/Products/Pizza/ru-RU/ec29465e-606b-4a04-a03e-da3940d37e0e.jpg",
        type: "thin",
        size: 26,
        count: 1,
      },
    ];
    const action = {
      type: clearCart.type,
    };
    const result = cartReducer({ items: pizzas, totalPrice: 1295 }, action);
    expect(result).toEqual({ items: [], totalPrice: 0 });
    expect(result).toMatchSnapshot();
  });
});