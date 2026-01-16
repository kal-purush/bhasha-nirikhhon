"use client";
import { useDispatch, useSelector } from "react-redux";
import { GlobalState, setRedirect } from "./features/global";
import { RootState } from "./store";
import { usePathname, useSearchParams } from "next/navigation";
import { useCallback, useEffect } from "react";

export default function useRedirect() {
  const dispatch = useDispatch();
  const redirect = useSelector(
    (state: RootState) => (state.global as GlobalState).redirect
  );
  const searchParams = useSearchParams();
  const pathname = usePathname();
  const searchRedirect = encodeURIComponent(searchParams.get("redirect") || "");

  const dispatchRedirect = useCallback(() => {
    const value = searchRedirect
      ? searchRedirect
      : pathname === "/"
      ? "dashboard"
      : pathname;

    dispatch(setRedirect(value));
  }, [searchRedirect, pathname]);

  useEffect(() => {
    if (pathname !== "/se-connecter") dispatchRedirect();
  }, [searchRedirect, pathname]);

  return undefined;
}