import { useQuery } from "@tanstack/react-query";
import { client } from "@/lib/rpc";

interface NotificationTag {
  id: string;
  name: string;
}

export function useGetNotificationTags() {
  return useQuery({
    queryKey: ["notification-tags"],
    queryFn: async () => {
      const response = await client.api.notifications.tag.$get();

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Failed to fetch notification tags");
      }

      const data: NotificationTag[] = await response.json();
      return data;
    },
  });
}