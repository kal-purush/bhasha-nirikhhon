import axios from "axios";
import md5 from "md5";

const baseUrl = import.meta.env.VITE_API_BASE_URL;
const publicKey = import.meta.env.VITE_API_KEY_PUBLIC;
const privateKey = import.meta.env.VITE_API_KEY_PRIVATE;

const currentTime = new Date().getTime();
const message = currentTime + privateKey + publicKey;

const hash = md5(message);

export const api = axios.create({
  baseURL: `${baseUrl}`,
});

export const authenticate = () => {
  return `&ts=${currentTime}&apikey=${publicKey}&hash=${hash}`;
};