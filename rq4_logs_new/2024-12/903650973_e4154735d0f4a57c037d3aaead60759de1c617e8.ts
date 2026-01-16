// hooks/useClickOutside.ts
import { useEffect, RefObject } from 'react';

type Event = MouseEvent | TouchEvent;

export const useClickOutside = <T extends HTMLElement | null>(
    ref: RefObject<T>,
    handler: (event: Event) => void
) => {
    useEffect(() => {
        const listener = (event: Event) => {
            // Kiểm tra xem phần tử ref có tồn tại không
            const el = ref.current;

            // Nếu click bên trong phần tử hoặc ref không tồn tại thì không làm gì
            if (!el || el.contains(event.target as Node) || null) {
                return;
            }

            // Gọi handler khi click ra ngoài
            handler(event);
        };

        // Thêm sự kiện mousedown và touchstart
        document.addEventListener('mousedown', listener);
        document.addEventListener('touchstart', listener);

        // Cleanup listener khi component unmount
        return () => {
            document.removeEventListener('mousedown', listener);
            document.removeEventListener('touchstart', listener);
        };
    }, [ref, handler]);
};