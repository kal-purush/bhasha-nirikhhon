import axios from "axios";

const axiosHttp = async function (request) {
    try {
        const res = await axios(request);
        return res.data;
    } catch (err) {
        return {
            ok: false,
            msg: `Error ${err.response.status} ${err.response.statusText || "an error occurred"
            }`,
        };
    }
};

export default axiosHttp;