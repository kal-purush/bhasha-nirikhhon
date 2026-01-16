import axios from 'axios';

const api = axios.create({
    baseURL: process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080/api',
    headers: {
        'Content-Type': 'application/json',
    },
    withCredentials: true
});

export const fetchProducts = async () => {
    try {
        const response = await api.get('/products');
        console.log('API response:', response.data);
        return response.data._embedded?.products ?? [];
    } catch (error) {
        console.error('Error fetching products:', error);
        return [];
    }
};

export default api;