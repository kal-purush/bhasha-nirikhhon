/**
 * Theme detection and management utilities
 */

// Define theme type
export type Theme = 'light' | 'dark' | 'system';

// Get system preference for dark mode
export function getSystemThemePreference(): 'light' | 'dark' {
  return window.matchMedia &&
    window.matchMedia('(prefers-color-scheme: dark)').matches
    ? 'dark'
    : 'light';
}

// Initialize theme based on system preference or stored setting
export function initializeTheme(): Theme {
  // Check if there's a stored theme preference
  const storedTheme = localStorage.getItem('theme') as Theme | null;

  if (
    storedTheme &&
    (storedTheme === 'light' ||
      storedTheme === 'dark' ||
      storedTheme === 'system')
  ) {
    return storedTheme;
  }

  // Default to system
  return 'system';
}

// Set the theme class on the document root
export function applyTheme(theme: Theme): void {
  const root = document.documentElement;
  const effectiveTheme =
    theme === 'system' ? getSystemThemePreference() : theme;

  // Remove existing theme classes
  root.classList.remove('light-theme', 'dark-theme');

  // Add the appropriate theme class
  root.classList.add(`${effectiveTheme}-theme`);

  // Store the theme preference
  localStorage.setItem('theme', theme);
}

// Set up listeners for system theme changes
export function setupThemeListeners(theme: Theme): void {
  if (theme === 'system') {
    const darkModeMediaQuery = window.matchMedia(
      '(prefers-color-scheme: dark)',
    );

    // Handle system theme changes
    const handleThemeChange = (e: MediaQueryListEvent) => {
      applyTheme('system');
    };

    // Modern browsers
    if (darkModeMediaQuery.addEventListener) {
      darkModeMediaQuery.addEventListener('change', handleThemeChange);
    } else {
      // Legacy support for older browsers
      darkModeMediaQuery.addListener(handleThemeChange);
    }
  }
}