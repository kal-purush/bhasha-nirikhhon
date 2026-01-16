import axios from "axios";

const instance = axios.create({
  headers: { "Content-Type": "application/json" },
  baseURL: "/api",
});

export default instance