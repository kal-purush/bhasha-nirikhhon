const {
  getUserCartItems,
  addItemsToCart,
  updateItemsInCart
} = require("../Service/cartItems.service");

const getCartItems = async (req, res) => {
  let { useremail } = req.user;
  try {
    let response = await getUserCartItems(useremail);

    return res.status(201).json({ message: response });
  } catch (error) {
    return res.status(400).json(error.message);
  }
};

const addItemtoCart = async (req, res) => {
  try {
    let cart = await addItemsToCart(
      req.body,
      req.body.productId,
      req.body.quantity,
    );
    return res.status(201).json({ message: cart });
  } catch (error) {
    return res.status(404).json({ message: error.message });
  }
};

const updateCartItem = async(req,res)=>{
    try {
        let cart = await updateItemsInCart(
          req.body,
          req.body.productId,
          req.body.quantity
        );
        return res.status(201).json({ message: cart });
      } catch (error) {
        return res.status(404).json({ message: error.message });
      }
}



module.exports = { getCartItems, addItemtoCart,updateCartItem };