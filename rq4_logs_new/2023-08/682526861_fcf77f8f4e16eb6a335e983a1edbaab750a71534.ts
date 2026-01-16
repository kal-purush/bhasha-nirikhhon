import { useEffect } from 'react';

export const useInfinitScroll = (refEl: any, cb: () => void) => {

    useEffect(() => {
        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting) {
                    cb();
                }
            },
            { threshold: 1 }
        );

        if (refEl.current) {
            observer.observe(refEl.current);
        }

        return () => {
            if (refEl.current) {
                observer.unobserve(refEl.current);
            }
        };
    }, [refEl]);
};