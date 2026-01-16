import ApiClient from "client/app/services";
import {ProductId, CreateProduct} from "./product-types";

const productsUrl = "/products";

const ProductsService = {
    getAll(limit: number = 20, offset: number = 0) {
        return ApiClient.get(productsUrl + `?limit=${limit}&offset=${offset}`)
    },

    getById(productId:ProductId) {
        return ApiClient.get(productsUrl + "/" + productId);
    },

    getByCategory(categoryName:string) {
        return ApiClient.get(productsUrl + '/category/' + categoryName)
    },

    create(data: CreateProduct) {
        return ApiClient.post(productsUrl, data);
    },

    update(productId: number, data: CreateProduct) {
        return ApiClient.put(productsUrl + "/" + productId, data);
    },

    delete(productId:number) {
        return ApiClient.delete(productsUrl + "/" + productId);
    }
}

export default ProductsService