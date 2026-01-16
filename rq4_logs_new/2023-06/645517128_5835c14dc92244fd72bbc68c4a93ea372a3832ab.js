import axios from "axios"

export const sanchezInstance = axios.create({
  baseURL: "http://localhost:5000/productos",
});