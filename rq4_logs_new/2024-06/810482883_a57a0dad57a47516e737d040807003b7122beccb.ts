import { Dispatch } from "@reduxjs/toolkit";
import { useEffect, useRef } from "react";
import { updateToken } from "../store/accessTokenSlice";
import { useAppDispatch } from "./useAppDispatch";

export const useAttachAccessToken = () => {
  const dispatch: Dispatch<any> = useAppDispatch();
  const url = useRef<URL>(new URL(window.location.href));
  useEffect((): void => {
    const accessToken =
      url.current.hash ||
      import.meta.env.VITE_REACT_DEFAULT_TOKEN.split("&")
        .find((x: string): string => x)
        ?.split("=")
        .pop();
    dispatch(updateToken({ accessToken }));
  }, []);
};