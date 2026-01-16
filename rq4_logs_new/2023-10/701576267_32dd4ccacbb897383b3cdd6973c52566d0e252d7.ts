"use client";

import { FakeAPIProduct } from "core/types/product";
import { CartItems } from "core/types/cartItem";
import { useContext } from "react";
import { LocalContext } from "core/contexts/localContext";

interface CartStore {
    id: number,
    quantity: number,
}
export const useCart = () => {    

    const {cart, updateCart} = useContext(LocalContext);    
    const {items, total} = cart;

    const addProduct = (product : FakeAPIProduct) => {
        const storageProduct = items.find((item) => item.id === product.id );  
        if(storageProduct){            
            const newValue: CartItems =  { 
                items: [...items.filter((p) => p.id !== product.id ), {id: product.id, quantity: storageProduct.quantity + 1}],                
                total: total + product.price
            }
            updateCart(newValue)            
        }
        else{
            const newValue: CartItems =  { 
                items: [...items, {id: product.id, quantity: 1}],                
                total: total + product.price
            }
            updateCart(newValue)
        }
    }

    const removeProduct = (product : FakeAPIProduct) => {
        const storageProduct = items.find((item) => item.id === product.id );  
        if(storageProduct){            
            const newValue: CartItems =  { 
                items: [...items.filter((p) => p.id !== product.id )],
                total: total + product.price
            }
            updateCart(newValue)            
        }       
    }

    return {cart, addProduct, removeProduct}
}