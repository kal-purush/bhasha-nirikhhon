import { useEffect, useState } from "react";

export function useFetch<T = unknown>(url: string, options?: RequestInit) {
    const [data, setData] = useState<T | null>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<Error | null>(null);

    useEffect(() => {
        let isMounted = true;
        setLoading(true);
        setError(null);

        fetch(url, options)
            .then(async (res) => {
                if (!res.ok) {
                    const text = await res.text();
                    throw new Error(text || `Error: ${res.status}`);
                }
                return res.json();
            })
            .then((data) => {
                if (isMounted) {
                    setData(data);
                }
            })
            .catch((err) => {
                if (isMounted) {
                    setError(err);
                }
            })
            .finally(() => {
                if (isMounted) {
                    setLoading(false);
                }
            });

        return () => {
            isMounted = false;
        };
    }, [url]);

    return { data, loading, error };
}