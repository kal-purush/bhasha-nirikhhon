import { useUserState } from '@/atoms/userAtom';
import { useSetCsrf } from '@/components/Login/useSetCsrf';
import { SignUp } from '@/types/form';
import { useRouter } from 'next/router';
import { useCallback, useEffect, useMemo } from 'react';
import { useCookies } from 'react-cookie';

export const useSignUp = () => {
    const { setUser } = useUserState();
    const { setCsrf } = useSetCsrf();
    const router = useRouter();
    const params = useMemo(() => new URLSearchParams(), []);
    const [cookies, setUseCookies] = useCookies(['_csrf']);

    useEffect(() => {
        setUseCookies('_csrf', cookies._csrf);
    }, [cookies._csrf, setUseCookies]);

    const signUp = useCallback(
        async (Inputs: SignUp) => {
            await setCsrf();

            params.append('email', Inputs.email);
            params.append('password', Inputs.password);
            fetch(`${process.env.NEXT_PUBLIC_API_URL}/signup`, {
                headers: {
                    'X-CSRF-TOKEN': cookies._csrf,
                },
                method: 'POST',
                mode: 'cors',
                credentials: 'include',
                body: params,
            })
                .then((response) => response.json())
                .then((data) => {
                    setUser(data);
                    router.push('/');
                })
                .catch((error) => {
                    setUser(null);
                    router.push('/sign_up');
                });
        },
        [cookies._csrf, params, router, setCsrf, setUser],
    );

    return { signUp };
};