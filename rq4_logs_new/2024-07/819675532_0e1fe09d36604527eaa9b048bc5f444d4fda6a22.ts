import { useQuery } from "@tanstack/react-query";
import { getCountries } from "../api/countryApi";

const useCountryQuery = () => {
  return useQuery({
    queryKey: ["countries"],
    queryFn: getCountries,
  });
};

export default useCountryQuery;