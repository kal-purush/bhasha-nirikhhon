import * as React from 'react';

type Theme = 'system' | 'light' | 'dark';

function useTheme() {
  const [theme, setTheme] = React.useState<Theme>();

  React.useEffect(() => {
    const handleClassChange = () => {
      setTheme(
        document.documentElement.classList.contains('dark') ? 'dark' : 'light'
      );
    };

    // Set initial theme based on the current classList
    handleClassChange();

    // Observe changes to the classList of the html element
    const observer = new MutationObserver(handleClassChange);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });

    // Cleanup the observer on component unmount
    return () => observer.disconnect();
  }, []);

  return { theme };
}

export { useTheme };