import axios from "axios";

const API_URL = "http://localhost:5000/api/bank";

export const getBalance = async () => {
    const response = await axios.get(`${API_URL}/balance`);
    return response.data.balance;
};

export const deposit = async (amount:number) => {
    const response = await axios.post(`${API_URL}/deposit`, { amount });
    return response.data;
};

export const withdraw = async (amount:number) => {
    const response = await axios.post(`${API_URL}/withdraw`, { amount });
    return response.data;
};