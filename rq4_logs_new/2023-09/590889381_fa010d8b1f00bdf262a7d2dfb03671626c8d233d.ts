import axios from 'axios';
import { Dispatch, SetStateAction } from 'react';
import { useRouter } from 'next/router';
import { useSetRecoilState } from 'recoil';
import { getUserSession } from '../../hooks/useQueryUser';
import { currentUserId } from '../../recoil/boardState';
import { toastSummary } from '../toastSummary';

export const loginFeature = (
  resetEmailValue: Dispatch<SetStateAction<string>>,
  resetPasswordValue: Dispatch<SetStateAction<string>>,
  email: string,
  password: string
) => {
  const router = useRouter();
  const { errorLoginToast, loginToast } = toastSummary();
  const setUserId = useSetRecoilState(currentUserId);
  const testSignIn = async () => {
    try {
      const testUserEmail: string = 'testUser@test.com';
      const testUserPassword: string = 'uidjopDHJilkjfskkkd84352';
      await axios.post(`${process.env.NEXT_PUBLIC_API_URL}/auth/login`, {
        email: testUserEmail,
        password: testUserPassword,
      });
      const user = await getUserSession();
      setUserId(user);
      loginToast();
      await router.push('/board');
    } catch (err) {
      errorLoginToast();
      console.log(err);
    }
  };

  const signInEmail = async () => {
    try {
      await axios.post(`${process.env.NEXT_PUBLIC_API_URL}/auth/login`, {
        email,
        password,
      });
      resetEmailValue('');
      resetPasswordValue('');
      const user = await getUserSession();
      setUserId(user);
      loginToast();
      await router.push('/board');
    } catch (err) {
      errorLoginToast();
      console.log(err);
    }
  };

  return { testSignIn, signInEmail };
};