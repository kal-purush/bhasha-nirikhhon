import {IWebService} from "./IWebService";
import axios from "axios";

/**
 * Web Service Client Axios Implementation
 */
const AxiosWebService = (): IWebService => {
    return {
        post: async (data, onSuccess, onFail) => {
           return await axios.post(data.url, data.data, data.config)
                .then(onSuccess)
                .catch(onFail);
        },
        get: async (data, onSuccess, onFail) => {
            return await axios.get(data.url, data.config)
                .then(onSuccess)
                .catch(onFail)
        }
    }
}

export default AxiosWebService