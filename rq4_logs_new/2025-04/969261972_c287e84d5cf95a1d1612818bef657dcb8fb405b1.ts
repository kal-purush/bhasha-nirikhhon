import { authAxios } from "@/app/lib/axios";
import { NotificationResponse } from "@/app/types/notificaitions";
import { useQuery } from "@tanstack/react-query";

export const getNotifications = async () => {
  const response = await authAxios.get<NotificationResponse[]>("/notification");
  return response.data;
};

export const useNotifications = () => {
  return useQuery({
    queryKey: ["notifications"],
    queryFn: () => getNotifications()
  });
};