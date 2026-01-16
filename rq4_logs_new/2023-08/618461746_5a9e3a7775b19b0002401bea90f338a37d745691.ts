import { useMutation, useQueryClient } from '@tanstack/react-query';

import { auth, common } from '@/models';

// TODO: change to real api
export const login = async (payload: auth.LoginPayload) => {
  //   const url = 'api/auth/login';
  // Testing purpose
  if (payload.email === '1') {
    return Promise.resolve({
      id: 1,
      name: 'John Doe',
      email: 'admin@test.com',
      citizenIdentification: '1234567890',
      roleID: 1,
      jobPositionID: 1,
      roleName: 'Administrator',
      jobPositionName: 'Manager'
    });
  }

  // if (payload.email !== 'officer@test.com')
  return Promise.resolve({
    id: 1,
    name: 'John Doe',
    email: 'officer@test.com',
    citizenIdentification: '1234567890',
    roleID: 2,
    jobPositionID: 1,
    roleName: 'Administrator',
    jobPositionName: 'Manager'
  });
};

export const useLogin = ({ onSuccess, onError }: common.useMutationParams) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: auth.LoginPayload) => login(payload),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['Login'] });
      onSuccess?.(data);
    },
    onError: () => {
      onError?.();
    }
  });
};