import { useEffect, useState } from "react";
import { serialize, parse } from "cookie";
import { CookieSerializeOptions } from "next/dist/server/web/types";

const useCookies = (cookieName: string) => {
  const [activeCookie, setActiveCookie] = useState<any>({});

  const setData = (cookieData: any) => {
    try {
      const options: CookieSerializeOptions = { sameSite: "lax" };
      const newCookie = serialize(
        cookieName,
        JSON.stringify(cookieData),
        options
      );

      document.cookie = newCookie;
      if (!cookieData || !cookieData["id"]) return;
      setActiveCookie(cookieData);
    } catch (e) {
      console.error(`Error setting new cookie ${e}`);
    }
  };

  const getCookieByKey = (key: string): any => {
    if (typeof window === "undefined") return false;
    try {
      const cookies = parse(document.cookie);
      if (cookies[key]) return JSON.parse(cookies[key]);
    } catch (e) {
      console.error(`Error retrieving cookie by key ${e}`);
    }

    return { id: 0 };
  };

  useEffect(() => {
    try {
      const cookies = parse(document.cookie);
      if (!cookies.cookieName) return;
    } catch (e) {
      console.error(`Error parsing cookies in hook ${e}`);
    }
  }, [cookieName]);

  return {
    activeCookie,
    getCookieByKey,
    setData,
  };
};

export default useCookies;