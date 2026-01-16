import { useEffect } from 'react';
import { useRouter } from 'next/router';
import { useDispatch } from 'react-redux';
import { FetchBaseQueryError } from '@reduxjs/toolkit/dist/query';
import type { SerializedError } from '@reduxjs/toolkit';

import { logout } from '@/redux/features/auth/authSlice';
import { PATH_LOGIN } from '@/constants';

const useLogoutOnError = (
  isError: boolean,
  error: FetchBaseQueryError | SerializedError | undefined
) => {
  const dispatch = useDispatch();
  const router = useRouter();
  useEffect(() => {
    if (error && 'status' in error && !('error' in error)) {
      const errMsg = JSON.stringify(error.data);
      if (errMsg.toLowerCase().includes('token')) {
        dispatch(logout());
        router.push(`/${PATH_LOGIN}`);
      }
    }
  }, [isError, error]);
};

export default useLogoutOnError;