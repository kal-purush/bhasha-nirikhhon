import React from 'react';
import { useStateValue } from './StateProvider';
import "./Checkout.css";
import CheckoutProduct from './CheckoutProduct';

const Checkout = () => {
    const [{basket}]=useStateValue();
    return (
        <div className="checkout">
            <img
             className="checkout_ad"
             src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRBt_sebcgtsL6o64iSnFQUyuSF13ldSnH_Aw&usqp=CAU"
             alt="chechkout ad"
             />
             {basket?.length===0 ? (
                 <div>
                     <h2>Your Shopping basket is emty</h2>
                     <p>Your Sopping basket is empty to buy one or more click on "add to basket"</p>
                 </div>
             ):
             (
                 <div>
                     <h2>Your Shopping Basket</h2>
                     {basket.map(item=>(
                         <CheckoutProduct
                         item={item.id}
                         title={item.title}
                         image={item.image}
                         price={item.price}
                         rating={item.rating}
                         />
                     ))}
                 </div>
             )
             }
        </div>
    );
};

export default Checkout;