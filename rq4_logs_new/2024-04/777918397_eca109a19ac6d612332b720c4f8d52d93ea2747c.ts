import axios from "axios";


class Axios {

    public async getContentfulData(query: string) {
        const apiUrl: string = "https://graphql.contentful.com/content/v1/spaces/aliqirq3u5g5/";
        const response = await axios.post(apiUrl, JSON.stringify({ query }), {
            headers: {
                "Content-Type": "application/json",
                // Authenticate the request
                Authorization: "Bearer KAljMffebxLiF5bmndoYxDl2SCgdaMwsi0mxGHD2tdg",
            }
        });
        const { data, status } = response;
        return { data, status };
    }

    public async getAllData(apiCall: string) {
        const apiUrl: string = "https://localhost:7250/" + apiCall;
        const response = await axios.get(apiUrl);
        const { data, status } = response;
        return { data, status };
    }

    public async deleteData(itemId: string, apiCall: string) {
        const apiUrl: string = "https://localhost:7250/" + apiCall + "/" + itemId;
        const response = await axios.delete(apiUrl);
        const { data, status } = response;
        return { data, status };
    }

    public async postData(apiSuffix: string, postData: object) {
        const apiUrl: string = "https://localhost:7250/" + apiSuffix;
        const { data, status } = await axios.post(apiUrl, postData);
        return { data, status };
    }

    public async putData(apiSuffix: string, itemId: string, putData: object) {
        const apiUrl: string = "https://localhost:7250/" + apiSuffix + "/" + itemId;
        const { data, status } = await axios.put(apiUrl, putData);
        return { data, status };
    }

}

const axiosApi: Axios = new Axios();
export default axiosApi;