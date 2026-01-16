import axios from "axios";

export default class Api {

    static token = localStorage.getItem("token");

    static axiosInstance = axios.create({
        baseURL: process.env.REACT_APP_API_URL || "http://localhost:8090",
        headers: {'Content-Type' : 'application/json', "Authorization": "Bearer " + Api.token}
    });

    static getSpotifyUrl() {
        return this.axiosInstance.get("/spotify/url")
    }

    static registerSpotify(code: string, state: string) {
        return this.axiosInstance.post("/spotify/register", {
            code,
            state
        })
    }

    static registerAftg(key: string) {
        return this.axiosInstance.post("/aftg/register", {
            key
        })
    }

    static async recordActivty(enabled: boolean) {
        return this.axiosInstance.put("/record-activity", {enabled});
    }

    static async login(email: string, pwd: string) {
        try {
            const response = await this.axiosInstance.post("/login", {
                email,
                password: pwd
            });
            this.token = response.data.token;
            this.axiosInstance.defaults.headers.Authorization = "Bearer " + this.token;
            localStorage.setItem("token", String(this.token));

        } catch (e) {
            console.log(e);
        }
    }

    static async getApiKeys() {
        return this.axiosInstance.get("/connectors");
    }
}