import { useContext } from 'react';

import { LOCAL_STORAGE_THEME_KEY, Theme } from '@/providers/ThemeProvider/constants';
import { ThemeContext } from '@/providers/ThemeProvider/context';

interface UseThemeResult {
	theme: Theme;
	toggleTheme: (saveAction: (theme: Theme) => void) => void;
}

export function useTheme(): UseThemeResult {
	const { theme, setTheme } = useContext(ThemeContext);

	const toggleTheme = (saveAction: (theme: Theme) => void) => {
		let newTheme: Theme = Theme.LIGHT;

		switch (theme) {
			case Theme.LIGHT:
				newTheme = Theme.PINK;
				break;
			case Theme.PINK:
				newTheme = Theme.LIGHT;
				break;
		}

		localStorage.setItem(LOCAL_STORAGE_THEME_KEY, newTheme);

		setTheme?.(newTheme);

		saveAction?.(newTheme);
	};

	return {
		theme: theme || Theme.LIGHT,
		toggleTheme
	};
}