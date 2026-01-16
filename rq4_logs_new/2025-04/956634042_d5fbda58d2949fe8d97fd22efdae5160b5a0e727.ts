import { useQuery } from "@tanstack/react-query";
import { useAuthStore } from "../store/authStore";
import { useNotificationsStore } from "../store/notificationsStore";
import { notificationsService } from "../services/notifications";

const POLLING_INTERVAL = 10000; // Poll every 10 seconds

export const useNotifications = () => {
  const accessToken = useAuthStore((state) => state.accessToken);
  const setHasNotifications = useNotificationsStore(
    (state) => state.setHasNotifications
  );

  return useQuery({
    queryKey: ["notifications"],
    queryFn: async () => {
      const hasNotifications = await notificationsService.getNotifications(
        accessToken
      );
      setHasNotifications(hasNotifications);
      return hasNotifications;
    },
    refetchInterval: POLLING_INTERVAL,
    enabled: !!accessToken,
  });
};