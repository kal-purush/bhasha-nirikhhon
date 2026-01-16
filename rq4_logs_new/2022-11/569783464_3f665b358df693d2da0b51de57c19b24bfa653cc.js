import axios from "axios";

export default axios.create({
    baseURL: "http://localhost:8000/api",
    // baseURL:"http://localhost/mhp-pharma-ecommerce/server/public/api",
    // baseURL:"http://pos.greatdoc.org/pos_server/public//api",
    headers: {
        "Content-type": "application/json"
    }
});