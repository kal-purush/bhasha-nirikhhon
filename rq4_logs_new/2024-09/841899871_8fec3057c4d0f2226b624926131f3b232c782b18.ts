import axiosInstance from '@/services/base';
import { BASE_API_URL } from '@/services/constants';
import {
  ResetPasswordRequest,
  ResetPasswordResponse,
  SendResetPasswordEmailRequest,
  SendResetPasswordEmailResponse,
  VerifyResetPasswordTokenRequest,
  VerifyResetPasswordTokenResponse,
} from '@/services/password-resets/types';

const baseAPI = `${BASE_API_URL}/password_resets`;

const passwordResetsService = {
  sendResetPasswordEmail: async (
    data: SendResetPasswordEmailRequest
  ): Promise<SendResetPasswordEmailResponse> => {
    const response = await axiosInstance.post(`${baseAPI}/reset`, data);
    return response.data;
  },
  verifyResetPasswordToken: async (
    data: VerifyResetPasswordTokenRequest
  ): Promise<VerifyResetPasswordTokenResponse> => {
    const response = await axiosInstance.get(`${baseAPI}/verify/${data.token}`);
    return response.data;
  },
  resetPassword: async (
    data: ResetPasswordRequest
  ): Promise<ResetPasswordResponse> => {
    const response = await axiosInstance.patch(
      `${baseAPI}/update_password`,
      data
    );
    return response.data;
  },
};

export default passwordResetsService;