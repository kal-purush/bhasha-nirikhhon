import { FakeAPIProduct } from "core/types/product";
import { useFilterStore } from "../useFilter/useFilter";
import { Order } from "core/types/order";

export const useProducts = (products: FakeAPIProduct[]) => {
    const { filter, order } = useFilterStore();
    let filteredProducts =
        filter != ""
            ? products.filter((product) =>
            product.title.toLowerCase().includes(filter.toLowerCase())
            )
            : products;

    filteredProducts = order != Order.DEFAULT ? filteredProducts.sort((a, b) => {
        if (order ===  Order.ASC) {
            return a.price - b.price;
        } else {
            return b.price - a.price;
        }
    }) : filteredProducts;
    
    return filteredProducts;
}