import { DEFAULT_CONTENT_TYPE, TOKEN_KEY } from "@/constants/common";
import axios from "axios";
import { getCookie } from 'cookies-next';


const Axios = axios.create({
    baseURL: `${process.env.NEXT_PUBLIC_BASE_URL}`
});

Axios.interceptors.request.use((config: any) => {
    const token = getCookie(TOKEN_KEY);
    const headers = config.headers;

    return {
        ...config,
        headers: {
            'Content-Type': headers['Content-Type'] ?? DEFAULT_CONTENT_TYPE,
            'Authorization': token && `Bearer ${token}`
        },
    };

});

export default Axios;