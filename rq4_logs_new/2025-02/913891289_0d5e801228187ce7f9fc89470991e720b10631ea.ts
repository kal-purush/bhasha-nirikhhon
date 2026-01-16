"use client"

import { useState, useEffect, useCallback } from "react"
import type { ProductModels } from "@/types/product"
import ProductService from "@/services/product-service"

const useProducts = () => {
    const [products, setProducts] = useState<ProductModels[]>([])
    const [loading, setLoading] = useState<boolean>(false)
    const [error, setError] = useState<string | null>(null)

    const fetchAllProducts = useCallback(async () => {
        setLoading(true)
        setError(null)
        try {
            const productService = ProductService.getInstance()
            const response = await productService.getAllProductFood()
            if (response.success && response.result.items) {
                setProducts(response.result.items)
            } else {
                setProducts([])
            }
        } catch (err) {
            setError("Failed to fetch products")
            console.error(err)
        } finally {
            setLoading(false)
        }
    }, [])

    useEffect(() => {
        fetchAllProducts()
    }, [fetchAllProducts])


    return { products, loading, error }
}

export default useProducts
