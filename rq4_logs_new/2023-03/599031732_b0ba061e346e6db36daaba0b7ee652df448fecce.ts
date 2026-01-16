import axios, { AxiosResponse } from "axios";
import Address from "../../../model/Address";

export const geocode = (
  address: Address,
  onSuccess: (value: AxiosResponse) => void,
  onError: (reason: any) => void
) => {
  axios({
    baseURL: "https://geocode.search.hereapi.com/v1",
    url: "geocode",
    params: {
      apiKey: "TaSbg-2Wqc4JKrWsP5vT1OGCUnMFrAKf16_LvtjmACI",
      q:
        address.street +
        ", " +
        address.district +
        ", " +
        address.city +
        ", " +
        (address.province ? `${address.province}, ` : "") +
        address.country,
    },
  })
    .then(onSuccess)
    .catch(onError);
};