import { useQuery } from "$hooks";
import { GET_ADMIN_TASKS } from "$queries";
import { IAdminTasksFilter, TAdminTask } from "$interfaces/AdminTask";
import { ApolloQueryResult } from "@apollo/client";
import { useState } from "react";
import { uniqBy } from "lodash";

export const useAdminTasks = (filter: IAdminTasksFilter) => {
  const defaultFilter = {
    adminTaskTypes: [],
    statuses: [],
    updatedBeforeThan: undefined
  };
  const [previousTasks, setPreviousTasks] = useState<TAdminTask[]>([]);
  const result = useQuery<IAdminTasksFilter, IUseAdminTasks>(
    GET_ADMIN_TASKS,
    { variables: { ...defaultFilter, ...filter }, fetchPolicy: "no-cache" }
  );

  const fetchMore = async () => {
    if (!result.data) return;
    setPreviousTasks(uniqBy([...previousTasks, ...result.data.getAdminTasks.tasks], "uuid"));
    const tasks = result.data.getAdminTasks.tasks;
    return await result.refetch({
      ...defaultFilter,
      ...filter,
      updatedBeforeThan: tasks[tasks.length - 1].updatedAt
    });
  };

  const refetch = (newFilter: IAdminTasksFilter) => {
    setPreviousTasks([]);
    return result.refetch({ ...defaultFilter, ...newFilter });
  };

  return {
    ...result,
    fetchMore,
    refetch,
    data: (result.data && {
      getAdminTasks: {
        tasks: uniqBy([...previousTasks, ...result.data.getAdminTasks.tasks], "uuid"),
        shouldFetchMore: result.data.getAdminTasks.shouldFetchMore
      }
    })
  };
};

export type TRefetchGetAdminTasks = (
  filter: IAdminTasksFilter
) => Promise<ApolloQueryResult<IUseAdminTasks>>;

export interface IGetAdminTasks {
  tasks: TAdminTask[];
  shouldFetchMore: boolean;
}

interface IUseAdminTasks {
  getAdminTasks: IGetAdminTasks;
}