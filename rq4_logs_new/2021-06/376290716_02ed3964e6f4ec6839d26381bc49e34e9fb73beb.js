import axios from "axios";
require('dotenv').config()

const AxiosAdmin = axios.create({
    baseURL: process.env.REACT_APP_API_URL_ADMIN,
    headers: {
        "content-type": "application/json"
    }
});
export default AxiosAdmin;