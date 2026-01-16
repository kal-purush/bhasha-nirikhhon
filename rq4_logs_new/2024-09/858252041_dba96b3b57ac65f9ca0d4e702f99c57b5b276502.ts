import React from "react";

import { useLocation } from "./useLocation";
import { getAuthUrl } from "../api/getAuthUrl";
import { getAuthToken } from "../api/getAuthToken";

export const useAuthentication = () => {
  const location = useLocation();
  const [isAuthenticated, setIsAuthenticated] = React.useState(false);

  const login = async () => {
    await getAuthUrl();
  };

  const handleToken = async (code: string) => {
    const result = await getAuthToken(code);
    setIsAuthenticated(result.isAuthenticated);
  };

  React.useEffect(() => {
    // Parse id from referrer or url
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    if (code) {
      handleToken(code);
    }
  }, [location]);

  return { login, isAuthenticated };
};