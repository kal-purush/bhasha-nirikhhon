import { useState, useEffect } from "react";
import { useDebounce } from "./useDebounce";

interface CacheItem<T> {
  data: T;
  timestamp: number;
}

const cache = new Map<string, CacheItem<any>>();
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

export function useApi<T>(
  url: string,
  options: RequestInit = {},
  debounceMs = 0,
) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(false);

  const debouncedUrl = useDebounce(url, debounceMs);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        // Check cache
        const cached = cache.get(debouncedUrl);
        if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
          setData(cached.data);
          setLoading(false);
          return;
        }

        const response = await fetch(debouncedUrl, options);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();

        // Update cache
        cache.set(debouncedUrl, {
          data: result,
          timestamp: Date.now(),
        });

        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setLoading(false);
      }
    };

    if (debouncedUrl) {
      fetchData();
    }

    return () => {
      // Cleanup if needed
    };
  }, [debouncedUrl]);

  return { data, error, loading };
}