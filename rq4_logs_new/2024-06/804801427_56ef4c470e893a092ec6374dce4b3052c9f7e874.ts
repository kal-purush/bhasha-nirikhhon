import {useEffect, useRef} from "react";

export const useClickOutside = (setIsClose: any, onChange?: any) => {
    const rootEl: any = useRef(null);

    useEffect(() => {
        const onClick = (e: any) => {
            if (!rootEl.current?.contains(e.target)) {
                return setIsClose((prev: any) => {
                    if (prev) onChange && onChange()
                    return false
                })
            }
        };
        const onTab = (e: any) => {
            if (e.keyCode === 9)
                return rootEl.current?.contains(e.target) || setIsClose(false)
        }
        document.addEventListener('click', onClick);
        document.addEventListener('keyup', onTab);
        return () => {
            document.removeEventListener('click', onClick)
            document.removeEventListener('keyup', onTab)
        };
    }, []);

    return {rootEl}
}