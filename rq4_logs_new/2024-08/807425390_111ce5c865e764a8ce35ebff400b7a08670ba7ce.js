import axios from "axios";

var BASEURL_API;
if (process.env.SIMANANG_MODE == 'production') {
    BASEURL_API = 'https://simanang.sman1el.sch.id/';
} else if(process.env.SIMANANG_MODE == 'development') {
    BASEURL_API = 'https://staging-simanang.sman1el.sch.id/';
}else{
    BASEURL_API='http://localhost/';
}

const { CancelToken } = axios;
const source = CancelToken.source();
const instance = axios.create({
    baseURL: BASEURL_API,
    cancelToken: source.token,
    headers: {
        "Access-Control-Allow-Origin": "*",
    }
});

export default instance;