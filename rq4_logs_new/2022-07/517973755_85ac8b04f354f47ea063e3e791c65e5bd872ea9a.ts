import { useEffect, useState } from 'react';

export const useLocalStorage = <T>(key: string, initialValue: T) => {
   const [storedValue, setStoredValue] = useState<T>();

   useEffect(() => {
      if (initialValue) {
         localStorage.setItem(key, JSON.stringify(storedValue || ''));
      }

      if (!storedValue && localStorage.getItem(key)) {
         setStoredValue(JSON.parse(localStorage.getItem(key) as string));
      }
   }, [initialValue, key, storedValue]);

   return [storedValue, setStoredValue] as const;
};