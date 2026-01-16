import { useEffect, useState } from "react";

type ScrollDirection = "up" | "down";

interface UseScrollDirectionOptions {
  threshold?: number;
  initialDirection?: ScrollDirection;
}

export function useScrollDirection({
  threshold = 10,
  initialDirection = "up",
}: UseScrollDirectionOptions = {}): ScrollDirection {
  const [scrollDirection, setScrollDirection] =
    useState<ScrollDirection>(initialDirection);
  const [lastScrollY, setLastScrollY] = useState<number>(0);

  useEffect(() => {
    let ticking = false;

    const updateScrollDirection = () => {
      const scrollY = window.scrollY;

      if (Math.abs(scrollY - lastScrollY) < threshold) {
        ticking = false;
        return;
      }

      const direction: ScrollDirection = scrollY > lastScrollY ? "down" : "up";
      setScrollDirection(direction);
      setLastScrollY(Math.max(scrollY, 0));
      ticking = false;
    };

    const onScroll = () => {
      if (!ticking) {
        window.requestAnimationFrame(updateScrollDirection);
        ticking = true;
      }
    };

    window.addEventListener("scroll", onScroll);

    return () => window.removeEventListener("scroll", onScroll);
  }, [lastScrollY, threshold]);

  return scrollDirection;
}