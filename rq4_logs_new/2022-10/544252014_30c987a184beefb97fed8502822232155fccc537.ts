
import React, { createContext } from 'react';

const CartContext = {
    items:[],
    totalAmount: 0,
    addItems:( type: any, item: any) => {},
    removeItems: (type: any, id: any) => {}
    }
export type CartState = typeof CartContext;
const CartCtx = createContext<typeof CartContext>(CartContext);

export default CartCtx;