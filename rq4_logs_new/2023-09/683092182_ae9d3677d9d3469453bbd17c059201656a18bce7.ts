import { useEffect } from 'react';
import { useRouter } from 'next/router';

export default function usePreventBackward() {
    const router = useRouter();

    useEffect(() => {
        router.beforePopState(({ url, as, options }) => {
            if (as !== router.asPath) {
                window.history.pushState('', '');
                router.push(router.asPath);
                return false;
            }

            return true;
        });
        return () => {
            router.beforePopState(() => true);
        };
    }, []);

    return <p>Welcome to the page</p>;
}