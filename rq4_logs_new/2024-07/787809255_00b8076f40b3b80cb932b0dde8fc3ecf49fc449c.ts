import { useCallback, useContext } from 'react';
import { useDispatch, useSelector } from 'react-redux';

import { CartCountContext } from '@/contexts/cart-count-context';
import { EHttpStatusCode } from '@/services/http-status-code';
import { selectIsAuthorized } from '@/store/auth/slice';
import type { LoginParameters } from '@/store/auth/thunks';
import { login, logout, tokenVerify } from '@/store/auth/thunks';
import type { AppDispatch } from '@/store/store';
import type { RejectedThunk } from '@/store/typedef';

export const useAuth = () => {
    const dispatch = useDispatch<AppDispatch>();

    const { updateCount } = useContext(CartCountContext);

    const isAuthorized = useSelector(selectIsAuthorized);

    const onLogin = useCallback(
        async (form: LoginParameters) => {
            try {
                await dispatch(login(form)).unwrap();
                return { isAuthorized: true };
            } catch (error: unknown) {
                const thunkError = error as RejectedThunk;
                if (thunkError?.statusCode === EHttpStatusCode.UNAUTHORIZED) {
                    return { isAuthorized: false };
                }
            }
        },
        [dispatch],
    );

    const onLogout = useCallback(() => {
        dispatch(logout());
        updateCount();
    }, [dispatch, updateCount]);

    const verifyToken = useCallback(async () => {
        try {
            await dispatch(tokenVerify()).unwrap();
            return { isAuthorized: true };
        } catch (error: unknown) {
            const thunkError = error as RejectedThunk;
            if (thunkError?.statusCode === EHttpStatusCode.UNAUTHORIZED) {
                return { isAuthorized: false };
            }
        }
    }, [dispatch]);

    return { isAuthorized, onLogout, onLogin, verifyToken };
};