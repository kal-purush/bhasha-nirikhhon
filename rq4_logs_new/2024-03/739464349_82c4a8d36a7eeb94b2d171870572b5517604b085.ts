import { useEffect, useRef, MutableRefObject } from 'react';

export const useClickOutside = (callback: () => void) => {
	const ref: MutableRefObject<HTMLElement | undefined> = useRef();

	useEffect(() => {
		const handleClick = (event: MouseEvent) => {
			if (ref.current && !ref.current.contains(event.target as Node)) {
				callback();
			}
		};

		document.addEventListener('click', handleClick);

		return () => {
			document.removeEventListener('click', handleClick);
		};
	}, [ref, callback]);

	return ref;
};