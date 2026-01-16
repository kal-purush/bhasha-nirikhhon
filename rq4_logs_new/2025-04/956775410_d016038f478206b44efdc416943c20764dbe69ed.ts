import { useEffect } from "react";
import { useLocation } from "react-router";

const routeTitles: Record<string, string> = {
  "/": "SafulPay | Finance just got better",
  "/about-us": "SafulPay | About Us",
  "/privacy": "SafulPay | Privacy Policy",
  "/terms-and-condition": "SafulPay | Terms and Conditions",
};

export const usePageTitle = () => {
  const location = useLocation();

  useEffect(() => {
    const title = routeTitles[location.pathname] || "MyApp";
    document.title = title;
  }, [location.pathname]);
};