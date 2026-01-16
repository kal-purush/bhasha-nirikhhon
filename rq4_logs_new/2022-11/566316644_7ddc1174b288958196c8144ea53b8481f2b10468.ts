import { useEffect, useState } from 'react';
import axios, { AxiosError } from 'axios';
import { IProduct } from '../modules';

export function useProducts() {
    const [productsData, setProductsData] = useState<IProduct[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    async function fetchProducts() {

        try {
            setError('')
            setLoading(true);
            const responce = await axios.get<IProduct[]>('https://fakestoreapi.com/products');
            setProductsData(responce.data);
            setLoading(false)
        } catch (e: unknown) {
            const error = e as AxiosError;
            setLoading(false);
            setError(error.message)
        }
    }

    useEffect(() => {
        fetchProducts();
    }, [])

    return {productsData, loading, error}
}