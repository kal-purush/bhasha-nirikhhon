import { useCallback } from 'react';
import { useTheme } from '@mui/material';

const useScrollToSection = () => {
	const theme = useTheme();

	const scrollToSection = useCallback((title: string) => {
		const anchor = document.querySelector(
			`#${title.toLowerCase()}`
		) as HTMLElement;

		if (anchor) {
			const currentPosition = window.scrollY;
			let targetPosition = anchor.offsetTop;

			// when scrolling down -> needs and offset for the header
			if (targetPosition < currentPosition) {
				targetPosition -= Number(theme.mixins.toolbar.minHeight) ?? 0;
			}

			window.scrollTo({
				top: targetPosition,
				behavior: 'smooth',
			});
		}
	}, []);

	return { scrollToSection };
};

export default useScrollToSection;