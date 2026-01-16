import { useMutation, useQuery } from "@tanstack/react-query";
import axios from "axios";

interface Props {
  userId: string;
}

export default function useGroup({ userId }: Props) {
  const { mutateAsync: createGroup, isLoading: isCreateGroupLoading } = useMutation(["Create Group", userId], () =>
    postGroup(userId)
  );

  const { mutateAsync: createGroupWithGroupId, isLoading: isPutGroupLoading } = useMutation(
    ["Put Group"],
    (groupId: string) => putGroup(userId, groupId)
  );

  const {
    data,
    isLoading: isGetGroupLoading,
    refetch,
  } = useQuery(["group", userId], () => getGroupByUserId(userId), {
    enabled: !!userId,
  });

  const isLoading = [isCreateGroupLoading, isGetGroupLoading, isPutGroupLoading].some((x) => x);

  return {
    group: data?.data,
    createGroup,
    createGroupWithGroupId,
    isLoading,
    refetch,
  };
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

const postGroup = (userId: string) => {
  return axios.post(`/api/group/${userId}`);
};

const putGroup = (userId: string, groupId: string) => {
  return axios.put(`/api/group/${userId}`, { groupId });
};