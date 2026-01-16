import { AxiosGet } from "~/configs/axiosConfig";

const BANNER_KEY = {
  BANNER_ALL: "banner_all",
};


const getBanners = async () => {
  return await AxiosGet("/banners");
};

export { getBanners, BANNER_KEY };