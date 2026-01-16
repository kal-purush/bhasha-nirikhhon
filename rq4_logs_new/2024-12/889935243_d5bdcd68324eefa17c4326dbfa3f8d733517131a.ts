import axios from "axios";

const instance = axios.create({
    baseURL: "http://localhost:8000",
    // withCredentials: true
});

instance.interceptors.response.use((response) => {
    //         // Thrown error for request with OK status code
    return response;
});


export default instance;