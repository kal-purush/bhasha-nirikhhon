import { useEffect, useState } from 'react';

export function useScrollDirection() {
  const [scrollDirection, setScrollDirection] = useState<'down' | 'up'>('up');
  const [currentScrollY, setCurrentScrollY] = useState<number>(0);

  useEffect(() => {
    let lastScrollY = window.scrollY;

    let scrollTimeout: NodeJS.Timeout;

    const updateScrollDirection = () => {
      const { scrollY } = window;
      setCurrentScrollY(scrollY);

      const direction = scrollY > lastScrollY ? 'down' : 'up';

      if (
        direction !== scrollDirection &&
        // scroll direction sensitivity trigger
        (scrollY - lastScrollY > 10 || scrollY - lastScrollY < -5)
      ) {
        setScrollDirection(direction);
      }
      lastScrollY = scrollY > 0 ? scrollY : 0;

      if (scrollDirection === 'down') {
        // Clear previous timeout
        clearTimeout(scrollTimeout);
        // Set a new timeout to reset direction to "up" after 2 seconds
        scrollTimeout = setTimeout(() => {
          setScrollDirection('up');
        }, 2000);
      }
    };

    window.addEventListener('scroll', updateScrollDirection); // add event listener
    return () => {
      window.removeEventListener('scroll', updateScrollDirection); // clean up
      clearTimeout(scrollTimeout); // clear timeout on unmount
    };
  }, [scrollDirection]);

  return { scrollDirection, currentScrollY };
}