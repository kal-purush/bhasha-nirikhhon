// Adds item to cart in local storage
export const addItem = (item, next) => {
  let cart = [];
  if (typeof window !== undefined) {
    //if there is a cart started in local storage
    //cart becomes that data from local storage
    if (localStorage.getItem("cart")) {
      cart = JSON.parse(localStorage.getItem("cart"));
    }
    // push the new item to a variable with a count
    cart.push({
      ...item,
      count: 1,
    });
    // Creates a new Set (unique values) from cart and maps the
    //ids from that Set to a new array and returns the actual product
    //by running map() on the new array
    cart = Array.from(new Set(cart.map((p) => p._id))).map((id) => {
      return cart.find((p) => p._id === id);
    });
    //sets local storage to the value of the new cart
    localStorage.setItem("cart", JSON.stringify(cart));
    // Callback
    next();
  }
};