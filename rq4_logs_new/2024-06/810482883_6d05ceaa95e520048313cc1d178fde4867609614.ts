import { useAppSelector } from "./useAppSelector";
import axios from "axios";
import { YandexIdResponse } from "../types";
import { useEffect } from "react";
import { useAppDispatch } from "./useAppDispatch";
import { updateInfo } from "../store/YandexIDSlice";

export const useYandexIdInfo = (): void => {
  const dispatch = useAppDispatch();
  const accessToken = useAppSelector(
    (state: any) => state.accessToken.accessToken
  );

  const getData = async (): Promise<void> => {
    const url = import.meta.env.VITE_REACT_YANDEX_ID_URL;
    const params = {
      format: "json",
    };
    const headers = {
      Authorization: `OAuth ${accessToken}`,
    };
    const data: YandexIdResponse = await axios
      .get(url, { params, headers })
      .then((response) => response.data)
      .catch((err) => console.log(err));
    Boolean(data) && dispatch(updateInfo(data));
  };

  useEffect((): void => {
    Boolean(accessToken) && getData();
  }, [accessToken]);
};