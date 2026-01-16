import { create } from "zustand";
import { UserProps } from "@/interfaces/user";
import { useFetchUsersQuery } from "@/hooks/request/useUserRequest";

type UserState = {
  users: UserProps[] | null;
  isLoading: boolean;
};

type UserAction = {
  setUsers: (users: UserProps[]) => void;
  setLoading: (isLoading: boolean) => void;
};

const useUserStore = () => {
  const { data, isFetching } = useFetchUsersQuery({});
  const { getState, setState } = create<UserState & UserAction>((set) => ({
    users: null,
    isLoading: false,
    setUsers: (users: UserProps[]) => set({ users }),
    setLoading: (isLoading: boolean) => set({ isLoading })
  }));

  // Update the store state based on the query result
  setState((prev) => ({
    ...prev,
    users: data?.data || null,
    isLoading: isFetching
  }));

  return getState();
};

export default useUserStore;