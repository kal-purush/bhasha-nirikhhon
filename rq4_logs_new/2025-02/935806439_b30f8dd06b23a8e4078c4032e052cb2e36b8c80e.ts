import { useEffect, useState } from "react";
import { fetchProducts } from "../api/products";
import { addProduct } from "../api/products";  // Import hàm thêm sản phẩm từ service
import { useNavigate } from "react-router-dom";  // Sử dụng useNavigate để điều hướng

export interface Product {
    id: number;
    name: string;
    description: string;
    price: number;
    stock: number;
    image: string | null;
}

export const useProducts = () => {
    const [products, setProducts] = useState<Product[]>([]);
    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);

    const loadProducts = async () => {
        setLoading(true);
        try {
            const data = await fetchProducts();
            setProducts(data.data);
        } catch (err) {
            setError("Lỗi khi tải sản phẩm");
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        loadProducts();
    }, []);

    return { products, loading, error };
};


export const useAddProduct = (onProductAdded: () => void) => {
    const [name, setName] = useState("");
    const [description, setDescription] = useState("");
    const [price, setPrice] = useState(0);
    const [stock, setStock] = useState(0);
    const [image, setImage] = useState<File | null>(null);
    const navigate = useNavigate();  // Khởi tạo useNavigate

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            const formData = new FormData();
            formData.append("name", name);
            formData.append("description", description);
            formData.append("price", price.toString());
            formData.append("stock", stock.toString());
            if (image) {
                formData.append("image", image);  // Thêm ảnh vào FormData
            }

            const response = await addProduct(formData);  // Gọi API thêm sản phẩm
            alert(response.message);
            onProductAdded();  // Refresh danh sách sản phẩm
            navigate("/product-list");  // Điều hướng đến trang Product List

        } catch (error: any) {
            console.error("Lỗi khi thêm sản phẩm:", error.response?.data || error.message);
        }
    };

    return {
        name,
        description,
        price,
        stock,
        image,
        setName,
        setDescription,
        setPrice,
        setStock,
        setImage,
        handleSubmit,
    };
};