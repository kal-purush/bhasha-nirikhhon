import { useQuery } from "@tanstack/react-query";

import { client } from "@/lib/rpc";
import { authClient } from "@/lib/auth-client";

interface FilterParams {
  page?: number;
  limit?: number;
  search?: string | null;
}

export const useGetTeachers = (params: FilterParams) => {
  const activeOrganization = authClient.useActiveOrganization();
  const { page = 1, limit = 10, search = "" } = params;

  const query = useQuery({
    queryKey: ["teachers", { page, limit, search, activeOrganization }],
    queryFn: async () => {
      const queryParams = {
        page: page.toString(),
        limit: limit.toString(),
        ...(search && { search })
      };

      const response = await client.api.teachers.$get({
        query: queryParams
      });

      if (!response.ok) {
        const { message } = await response.json();

        throw new Error(message);
      }

      const data = await response.json();

      return data;
    }
  });

  return query;
};