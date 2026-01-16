import axios from 'axios';
const { NEXT_PUBLIC_BE_HOST } = process.env;

export const getTicketAxios = (token) => {
   const URL = `${NEXT_PUBLIC_BE_HOST}/payments/tickets/52f562fb-33b9-469f-b676-4ad064cf6192`;
   return axios.get(URL, { headers: { Authorization: `Bearer ${token}` } });
};