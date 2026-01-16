import {
  useMutation,
  UseMutationResult,
  useQueryClient,
} from '@tanstack/react-query';
import axios from 'axios';
import { useRouter } from 'next/router';
import { destroyCookie } from 'nookies';
import { useSetRecoilState } from 'recoil';
import { USER } from '../consts/queryKey';
import { currentUserId } from '../recoil/boardState';
import { userProfile, avatar } from './types/queryType';

export const useMutateUser = (): {
  deleteUserMutation: UseMutationResult<void, any, void, unknown>;
  updateAvatarMutation: UseMutationResult<
    void,
    unknown,
    Omit<avatar, 'id'>,
    unknown
  >;
} => {
  const queryClient = useQueryClient();
  const router = useRouter();
  const setUser = useSetRecoilState(currentUserId);

  const updateAvatarMutation = useMutation(
    async (avatar: Omit<avatar, 'id'>) => {
      await axios.patch(
        `${process.env.NEXT_PUBLIC_API_URL}/user/avatar`,
        avatar
      );
    },
    {
      onMutate: async () => {
        await queryClient.cancelQueries([USER]);
        const previousData = queryClient.getQueryData<userProfile>([USER]);

        queryClient.setQueryData([USER], { ...previousData });

        return { previousData };
      },
      onError: (err: any, context) => {
        if (err.response.status === 401 || err.response.status === 403) {
          console.log(err);
          router.push('/board');
        }
      },
      onSettled: () => {
        queryClient.invalidateQueries([USER]);
      },
    }
  );

  const deleteUserMutation = useMutation(
    async () => {
      await axios.delete(
        `${process.env.NEXT_PUBLIC_API_URL}/user/delete_profile`
      );
    },
    {
      onSuccess: async () => {
        queryClient.removeQueries([USER]);
        destroyCookie(null, USER, '/');
        setUser(undefined);
        await axios.post(`${process.env.NEXT_PUBLIC_API_URL}/auth/logout`);
        await router.push('/');
      },
      onError: (err: any) => {
        if (err.response.status === 401 || err.response.status === 403) {
          console.error(err);
          router.push('/board');
        }
      },
    }
  );

  return { deleteUserMutation, updateAvatarMutation };
};