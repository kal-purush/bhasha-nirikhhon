import React from 'react';
import './Cart.css';

const Cart = ({cart}) => {

    let itemsPrice = 0;
    cart.forEach((pd) => (itemsPrice += Number(pd.price)));
    let shippingPrice = 0;
    cart.forEach((pd) => (shippingPrice += Number(pd.shipping)));

    return (
        <div className="cart">
            <h3>Order Summery</h3>
            <p>Total item added: {cart.length}</p>
            <p>Items price: ${itemsPrice.toFixed(2)}</p>
            <p>Shipping price: ${shippingPrice.toFixed(2)}</p>
            <p>Tax: ${(itemsPrice * 0.05).toFixed(2)}</p>
            <h4 className="highlighted">Total: ${(itemsPrice + shippingPrice + itemsPrice * 0.05).toFixed(2)}</h4>
        </div>
    );
};

export default Cart;