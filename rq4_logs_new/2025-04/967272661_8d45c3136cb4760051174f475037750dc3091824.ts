import { useState, useEffect, useRef } from 'react';

export function useOutSideClickAutoClose(initialState: boolean) {
  const [isOpen, setIsOpen] = useState<boolean>(initialState);
  const ref = useRef<HTMLDivElement | null>(null);

  const handleOutsideClick = (e: MouseEvent) => {
    if (ref.current && !ref.current.contains(e.target as Node)) {
      setIsOpen(false);
    }
  };

  useEffect(() => {
    document.addEventListener('mousedown', handleOutsideClick, true);
    return () => {
      document.removeEventListener('mousedown', handleOutsideClick, true);
    };
  }, []);

  return { ref, isOpen, setIsOpen };
}