import {useCallback, useEffect, useRef, useState} from "react";

export const useFullScreenModal = ()=> {
    const ref = useRef<HTMLDivElement | null>(null);
    const [isFullScreen, toggleFullScreen] = useState(false);
    const handleClick = useCallback( ()=> {
        toggleFullScreen(prevState => {
            return !prevState;
        });
    }, []);

    useEffect(() => {
        const currentRef = ref.current;
        if (currentRef) {
            currentRef.addEventListener('click', handleClick);
        }

        return () => {
            if (currentRef) {
                currentRef.removeEventListener('click', handleClick);
            }
        };
    }, [handleClick]);

    return {ref, isFullScreen, toggleFullScreen};

};