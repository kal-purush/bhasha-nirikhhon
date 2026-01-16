import { useEffect, useState } from "react";
import { ErrorType } from "../components/Error/Type";
import { Debug } from "../lib/Debug";
import useCookies from "./useCookies";

type HandleErrorProps = {
  cookieName: string;
  propertyToValidate: string;
  pageProps?: any;
};

/**
 * @group Components
 * @param options
 */
function useErrors(options: HandleErrorProps) {
  const { getCookieByKey } = useCookies(options.cookieName);
  const { pageProps = {} } = options;
  const [error, setError] = useState<ErrorType<string>>({ error: false });
  const [selectedData, setSelectedData] = useState<string>("");

  useEffect(() => {
    const data = getCookieByKey(options.cookieName);
    const suppliedPageProps = Object.keys(pageProps).length !== 0;

    if (!data["id"]) {
      setError({
        error: true,
        code: "NoCookie",
        message: `Cookie was not found for key: ${options.cookieName}`,
      });
    }

    if (suppliedPageProps && pageProps["error"]) {
      setError({
        error: true,
        ...pageProps.error,
      });
    }

    if (suppliedPageProps && data["displayName"]) {
      setSelectedData(data["displayName"]);
    }

    const debug = Debug.init({
      debugSrc: "hooks/useErrors",
      data,
      debugType: "object",
    });
    debug.printDebugOutput();
  }, []);

  return error;
}

export default useErrors;