import { useQuery } from "@tanstack/react-query";
import axios from "axios";
import { useSession } from "next-auth/react";
import useGetGroup from "../group/useGetGroup";

export default function useGetPlan() {
  const { data: userData } = useSession();
  const userId = userData?.user?.id || "";
  const { data: gruop } = useGetGroup();
  const groupId = gruop?.userToGroup.groupId || "";

  const { isLoading, data } = useQuery(
    ["plan", userId, groupId],
    () => getSchedules(userId, groupId),
    {
      enabled: !!userId && !!groupId,
    }
  );

  return { isLoading, plans: data?.data };
}

interface Plan {
  title: string;
  startDate: string;
  endDate: string;
  isUndecided: boolean;
  content: string;
  groupId: string;
}
const getSchedules = (userId: string, groupId: string) => {
  return axios.get<Plan[]>(`/api/user/${userId}/group/${groupId}/plan`);
};