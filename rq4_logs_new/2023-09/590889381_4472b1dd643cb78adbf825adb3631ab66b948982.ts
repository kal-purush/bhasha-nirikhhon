import { useQueryClient } from '@tanstack/react-query';
import axios from 'axios';
import { useRouter } from 'next/router';
import { destroyCookie } from 'nookies';
import { useSetRecoilState } from 'recoil';
import { COMMENT, OWN_POSTING, POSTING, USER } from '../../consts/queryKey';
import { currentUserId } from '../../recoil/boardState';
import { toastSummary } from '../toastSummary';

export const logoutFeature = () => {
  const queryClient = useQueryClient();
  const router = useRouter();
  const setUser = useSetRecoilState(currentUserId);
  const { logoutToast } = toastSummary();

  const handleLogout = async () => {
    // キャッシュに格納されている情報をログアウト時に消去する
    queryClient.removeQueries([POSTING]);
    queryClient.removeQueries([COMMENT]);
    queryClient.removeQueries([USER]);
    queryClient.removeQueries([OWN_POSTING]);
    destroyCookie(undefined, USER, { path: '/' });
    setUser(undefined);
    logoutToast();
    await axios.post(`${process.env.NEXT_PUBLIC_API_URL}/auth/logout`);
    await router.push('/board');
  };

  return { handleLogout };
};