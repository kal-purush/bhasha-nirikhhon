import axios from "axios";

const API_URL = "http://localhost:8000/api/v1";

export const fetchProducts = async () => {
    try {
        const response = await axios.get(`${API_URL}/products`);
        return response.data;
    } catch (error) {
        console.error("Lỗi khi fetch sản phẩm:", error);
        throw error;
    }
};
export const addProduct = async (productData: {
    name: string;
    description?: string;
    price: number;
    stock: number;
    image?: string;
}) => {
    const response = await axios.post(`${API_URL}/products`, productData);
    return response.data;
};