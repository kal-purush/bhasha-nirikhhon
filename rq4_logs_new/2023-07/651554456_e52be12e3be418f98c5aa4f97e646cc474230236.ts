import { useQuery } from "@tanstack/react-query";
import axios from "axios";
import { useSession } from "next-auth/react";
import useGetGroup from "./useGetGroup";

export default function useGetMemory() {
  const { data: userData } = useSession();
  const userId = userData?.user?.id || "";
  const { data: gruop } = useGetGroup();
  const groupId = gruop?.userToGroup.groupId || "";

  const { isLoading, data } = useQuery(
    ["memory", userId, groupId],
    () => getMemories(userId, groupId),
    {
      enabled: !!userId && !!groupId,
    }
  );

  return { isLoading, memories: data?.data };
}

interface Memory {
  uploadedImageUrls: string[];
  title: string;
  location: string;
  content: string;
  date: Date;
}
const getMemories = (userId: string, groupId: string) => {
  return axios.get<Memory[]>(`/api/user/${userId}/group/${groupId}/memory`);
};