// import { api } from "./api.config";

// export const getAllProducts = (): Promise<any> => {
//   console.log("response");
//   try {
//     const response = api.get(`/products?page=1&limit=30`);
//     return response;
//   } catch (error: any) {
//     if (error.response) {
//       console.error(error.response.data);
//     }
//     throw error;
//   }
// };

import { api } from "../../../services/api.config";

export const getAllProducts = async (): Promise<any> => {
  try {
    const response = await api.get(`/products?page=1&limit=30`);
    return response.data;
  } catch (error: any) {
    if (error.response) {
      console.error(error.response.data);
    }
    throw error;
  }
};