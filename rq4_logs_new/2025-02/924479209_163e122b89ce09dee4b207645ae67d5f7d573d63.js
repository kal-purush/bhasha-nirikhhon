import { axiosInstance } from "../../utils/axiosInstance";

export const VendorRegister = async (data) => {
  console.log("data in service section", data);
  if (data instanceof FormData) {
    for (let [key, value] of data.entries()) {
      console.log(`${key}:`, value);
    }
  }
  const response=await axiosInstance.post('/vendor/register',data,{
    headers:{"Content-Type": "multipart/form-data"}
  })
};