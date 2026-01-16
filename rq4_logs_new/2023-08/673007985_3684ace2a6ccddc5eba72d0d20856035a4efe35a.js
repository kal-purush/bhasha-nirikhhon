import React, { useEffect } from 'react';
import Layout from '../components/Layout';
import { useProducts } from '../contexts/ProductContext';


const CartPage = () => {
    const {productState:{cart},dispatchProduct} = useProducts()

    const getCartProducts = async ()=>{
        const res = await fetch("/api/cart");
        const data = await res.json();
        
        dispatchProduct({type:"GET_CART_PRODUCTS",payload:data.cartProducts})

    }

    useEffect(()=>{
        getCartProducts();
    },[])

    const removeFromCart= async (product)=>{
        const res = await fetch(`/api/cart/${product._id}`,{
            method:"DELETE",
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({product})
        });
        const data = await res.json();
        console.log(data)
        dispatchProduct({type:"DELETE_CART_PRODUCTS",payload:data.product})
     


    }
    
    return (
       <Layout>
         {
            cart?.map(product=><div key={product?._id} className='m-2'>
            <img src= {product?.productImg} className="img-fluid" alt= {product?.productName} />
              <h4 className='p-2'>{product?.productName}</h4>
              <p className='p-2'>Price : <span className='actual-price'>{product?.productPrice}</span><span className='p-2'>{product?.productDiscountPrice}</span></p>
              <button type="button" onClick={ () => removeFromCart(product)} className="btn btn-dark m-2">Remove from Cart</button> <br/>
              
            </div>

            )
        } 

       </Layout>
    );
}

export default CartPage;