import { queryOptions } from "@tanstack/react-query";

type Granularity =
  | "GRANULARITY_UNSPECIFIED"
  | "SUB_PREMISE"
  | "PREMISE"
  | "PREMISE_PROXIMITY"
  | "BLOCK"
  | "ROUTE"
  | "OTHER";

const validateAddress = (address: string) =>
  queryOptions({
    queryKey: ["validateAddress", address] as const,
    queryFn: async ({ queryKey: [_, address] }) => {
      const response = await fetch(
        `https://addressvalidation.googleapis.com/v1:validateAddress?key=${import.meta.env.VITE_PUBLIC_GOOGLE_MAPS_API_KEY}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            address: {
              regionCode: "US",
              addressLines: [address],
            },
          }),
        }
      );
      const data = await response.json();

      const isComplete = Boolean(data?.result?.verdict?.addressComplete);
      const isValid = ["PREMISE", "PREMISE_PROXIMITY"].includes(
        data?.result?.verdict?.inputGranularity as Granularity
      );

      return isComplete && isValid;
    },
  });

export const mapsQueries = { validateAddress };