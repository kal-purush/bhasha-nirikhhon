import { useCurrentUser, IUseCurrentUser } from "./useCurrentUser";
import { TCurrentAdmin } from "../../CurrentAdmin";
import { ApolloError, FetchResult } from "@apollo/client";

export const useCurrentAdmin = () => {
  const response = useCurrentUser();
  const admin = response.data?.getCurrentUser?.admin;

  return {
    ...response,
    error: response.error || !admin,
    ...(admin && {
      data: {
        currentAdmin: admin
      }
    }),
    ...(!admin && {
      data: undefined
    })
  } as UseCurrentAdminResult;
};

type UseCurrentAdminResult = ILoadingCurrentAdmin | IErroredCurrentAdmin | ISuccessfulCurrentAdmin;

type ILoadingCurrentAdmin = {
  data: undefined;
  refetch: undefined;
  error: undefined;
  loading: true;
};

type IErroredCurrentAdmin = {
  data: undefined;
  refetch: undefined;
  error: ApolloError;
  loading: false;
};

type ISuccessfulCurrentAdmin = {
  error: false;
  loading: false;
  refetch: ((variables: {}) => FetchResult<IUseCurrentUser>);
  data: {
    currentAdmin: TCurrentAdmin
  };
};