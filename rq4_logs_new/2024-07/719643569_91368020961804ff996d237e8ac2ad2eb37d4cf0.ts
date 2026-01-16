import { supabase } from "@/config/supabase"
import { ProductsStore } from "../store/productsStore"
import { Product, UpdateProductType } from "@/types/productInterface"

export const productRouter = {
    async getProducts() {
        try {
            const { data: products, error } = await supabase.from("products").select()
            if (error) {
                return error.message
            }
            ProductsStore.setState({ products })
            return products
        } catch (error) {
            return error
        }
    },
    async getProductById(id: number) {
        if (!id) {
            return "Product ID is required"
        }
        try {
            const { data, error } = await supabase.from("products").select().eq("id", id).single()
            if (error) {
                return error.message
            }
            const product: Product = data
            ProductsStore.setState({ productById: product })
            return product
        } catch (error) {
            return error
        }
    },
    async updateProductData(id: number, newProductData: UpdateProductType) {
        if (!id || !newProductData) {
            return "Product ID and new product data is required, you can't edit product ID"
        }
        try {
            const { data: currentProductData } = await supabase.from("products").select().eq("id", id).single()
            const updatedProductData = { ...currentProductData, ...newProductData }
            const { status, error } = await supabase.from("products").update(updatedProductData).eq("id", id)
            if (error) {
                return error.message
            }
            this.getProducts()
            return status
        } catch (error) {
            return error
        }
    },
    async deleteProduct(id: number) {
        if (!id) {
            return "Product ID is required"
        }
        try {
            const { data: product } = await supabase.from("products").select().eq("id", id).single()
            if (!product || product.length == 0) {
                return "Invalid Product ID"
            }
            const { status, error } = await supabase.from("products").delete().eq("id", id)
            if (error) {
                return error.message
            }
            this.getProducts()
            return status
        } catch (error) {
            return error
        }
    },
    async addNewProduct(newProductData: Product) {
        if (!newProductData) {
            return "Product Data is required"
        }
        try {
            const { status, error } = await supabase.from("products").insert(newProductData).select()
            if (error) {
                return error.message
            }
            this.getProducts()
            return status
        } catch (error) {
            return error
        }
    }
}