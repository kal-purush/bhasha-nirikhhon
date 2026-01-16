import { useQuery } from "@tanstack/react-query";
import axios from "axios";
import { useSession } from "next-auth/react";

export default function useGetGroup() {
  const { data: userData } = useSession();
  const userId = userData?.user?.id || "";

  const { data, isLoading, refetch } = useQuery(["group", userId], () => getGroupByUserId(userId), {
    enabled: !!userId,
  });

  return { data: data?.data, isLoading: isLoading, refetch };
}

interface _Group {
  userToGroup: {
    id: string;
    userId: string;
    groupId: string;
  };
  count: number;
}

const getGroupByUserId = (userId: string) => {
  return axios.get<_Group>(`/api/group/${userId}`);
};