import { AxiosError } from 'axios';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import userProfileService, { userProfileQueryKeys } from '.';
import {
  CreateUserProfileRequest,
  CreateUserProfileResponse,
  GetUserProfileByUserIdResponse,
  UpdateUserProfileRequest,
  UpdateUserProfileResponse,
} from './types';

export const useUserProfileByUserId = (userId: string | undefined) => {
  const query = useQuery<GetUserProfileByUserIdResponse, AxiosError>(
    [userProfileQueryKeys.getUserProfileByUserId, userId],
    () =>
      userProfileService.getUserProfileByUserId({ userId: userId as string }),
    {
      enabled: !!userId,
      refetchOnWindowFocus: false,
      retry: false,
    }
  );

  return query;
};

export const useCreateUserProfile = () => {
  const queryClient = useQueryClient();
  const mutation = useMutation<
    CreateUserProfileResponse,
    AxiosError,
    CreateUserProfileRequest
  >(userProfileService.createUserProfile, {
    onSuccess: () =>
      queryClient.invalidateQueries(
        userProfileQueryKeys.getUserProfileByUserId
      ),
  });

  return mutation;
};

export const useUpdateUserProfile = () => {
  const queryClient = useQueryClient();
  const mutation = useMutation<
    UpdateUserProfileResponse,
    AxiosError,
    UpdateUserProfileRequest
  >(userProfileService.updateUserProfile, {
    onSuccess: () =>
      queryClient.invalidateQueries(
        userProfileQueryKeys.getUserProfileByUserId
      ),
  });

  return mutation;
};