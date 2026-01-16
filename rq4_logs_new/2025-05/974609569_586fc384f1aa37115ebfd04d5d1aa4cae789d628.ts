import { useQuery } from "@tanstack/react-query";

import { client } from "@/lib/rpc";

interface FilterParams {
  childId: string;
}

export const useGetChildFeedbacks = (params: FilterParams) => {
  const { childId } = params;

  const query = useQuery({
    queryKey: ["feedbacks", { childId }],
    queryFn: async () => {
      const response = await client.api.children[":id"].feedbacks.$get({
        param: { id: childId }
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