import { RefObject, useEffect, useState } from 'react';

const useDetectClose = (ref: RefObject<HTMLDivElement | null>, initialState: boolean) => {
  const [isOpen, setIsOpen] = useState(initialState);

  useEffect(() => {
    const pageClickEvent = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setIsOpen(!isOpen);
      }
    };

    if (isOpen) {
      window.addEventListener('click', pageClickEvent);
    }

    return () => {
      window.removeEventListener('click', pageClickEvent);
    };
  }, [isOpen, ref]);
  return [isOpen, setIsOpen];
};

export default useDetectClose;