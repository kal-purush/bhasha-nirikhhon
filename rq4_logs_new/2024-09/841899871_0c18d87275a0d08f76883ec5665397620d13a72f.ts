import axiosInstance from '@/services/base';
import { BASE_API_URL } from '@/services/constants';
import {
  CreateUserProfileRequest,
  CreateUserProfileResponse,
  GetUserProfileByIdRequest,
  GetUserProfileByIdResponse,
  UpdateUserProfileRequest,
  UpdateUserProfileResponse,
} from './types';

const baseAPI = `${BASE_API_URL}/user_profiles`;

const userProfileService = {
  getUserProfileById: async (
    payload: GetUserProfileByIdRequest
  ): Promise<GetUserProfileByIdResponse> => {
    const response = await axiosInstance.get(`${baseAPI}/${payload.id}`);
    return response.data;
  },
  createUserProfile: async (
    payload: CreateUserProfileRequest
  ): Promise<CreateUserProfileResponse> => {
    const response = await axiosInstance.post(`${baseAPI}`, payload);
    return response.data;
  },
  updateUserProfile: async (
    payload: UpdateUserProfileRequest
  ): Promise<UpdateUserProfileResponse> => {
    const { id, ...data } = payload;
    const response = await axiosInstance.put(`${baseAPI}/${id}`, data);
    return response.data;
  },
};

export const userProfileQueryKeys = {
  getUserProfileById: 'user_profile_by_id',
};

export default userProfileService;