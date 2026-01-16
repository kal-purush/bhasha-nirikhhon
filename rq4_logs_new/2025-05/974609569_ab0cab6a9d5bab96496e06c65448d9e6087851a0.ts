import { useQuery } from "@tanstack/react-query";

import { client } from "@/lib/rpc";

interface FilterParams {
  parentId: string;
}

export const useGetParent = (params: FilterParams) => {
  const { parentId } = params;

  const query = useQuery({
    queryKey: ["parents"],
    queryFn: async () => {
      const response = await client.api.users[":id"].$get({
        param: { id: parentId }
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