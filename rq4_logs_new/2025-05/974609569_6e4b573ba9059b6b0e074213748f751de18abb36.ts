import { useQuery } from "@tanstack/react-query";
import { client } from "@/lib/rpc";

interface GetPaymentsParams {
  page?: number;
  limit?: number;
  search?: string;
}

export function useGetPayments(params: GetPaymentsParams = {}) {
  const { page = 1, limit = 10, search = "" } = params;

  return useQuery({
    queryKey: ["payments", { page, limit, search }],
    queryFn: async () => {
      try {
        const queryParams: Record<string, string> = {
          page: String(page),
          limit: String(limit),
        };

        if (search) {
          queryParams.search = search;
        }

        const response = await client.api.payments.$get({
          query: queryParams,
        });

        if (!response.ok) {
          const error = await response.json();
          throw new Error(error.message || "Failed to fetch payments");
        }

        const data = await response.json();
        return data;
      } catch (error) {
        console.error("Error fetching payments:", error);
        throw error;
      }
    },
  });
}