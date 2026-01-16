import { Order } from "core/types/order";
import { FakeAPIProduct } from "core/types/product";


export const getComparator = (order: Order) => {
    
    switch(order){
        case Order.ASC:
            return (a: FakeAPIProduct,b : FakeAPIProduct) => a.price - b.price;
        case Order.DSC:
            return (a: FakeAPIProduct,b : FakeAPIProduct) => b.price - a.price;
        case Order.RATING:
            return (a: FakeAPIProduct,b : FakeAPIProduct) => b.rating.rate - a.rating.rate;
        default:
            return (a: FakeAPIProduct,b : FakeAPIProduct) => a.id - b.id;
    }

}