import { useMutation, UseMutationOptions } from "@tanstack/react-query";

import { changePassword } from "@/lib/api/auth";
import { ResponseErr, ResponseSuccess } from "@/types/api";

interface ChangePasswordParams {
  username: string;
  old_password: string;
  new_password: string;
}

interface ChangePasswordResponse {
  success: boolean;
  message: string;
}

export const useChangePasswordUser = (
  options?: UseMutationOptions<
    ResponseSuccess<ChangePasswordResponse>,
    ResponseErr,
    ChangePasswordParams
  >,
) => {
  return useMutation({
    mutationFn: ({
      username,
      old_password,
      new_password,
    }: ChangePasswordParams) =>
      changePassword(username, old_password, new_password),
    ...options,
  });
};