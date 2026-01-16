import axios, {AxiosError, AxiosInstance, InternalAxiosRequestConfig} from "axios";

// Get API URL from environment variables
const baseURL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3000";

// Base axios instance without authentication
export const baseAxios = axios.create({
    baseURL,
    headers: {
        "Content-Type": "application/json",
    },
});

// Authenticated axios instance
export const authAxios: AxiosInstance = axios.create({
    baseURL,
    headers: {
        "Content-Type": "application/json",
    },
});

// Request interceptor to add the token from localStorage
authAxios.interceptors.request.use(
    (config: InternalAxiosRequestConfig) => {
        // Only run on client side
        if (typeof window !== "undefined") {
            const token = localStorage.getItem("accessToken");
            if (token) {
                config.headers.Authorization = `Bearer ${token}`;
            }
        }
        return config;
    },
    (error: AxiosError) => {
        return Promise.reject(error);
    }
);
