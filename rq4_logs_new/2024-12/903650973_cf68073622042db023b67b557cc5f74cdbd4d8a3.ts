import React from 'react';

interface User {
    username: string;
    email: string;
    role: string;
    hashedPassword: string;
    createdAt: string;
    id: number;
    isActive: boolean;
    isEmailConfirmed: boolean;
}

const useUser = (url: string) => {
    const [data, setData] = React.useState<User | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState(null);

    React.useEffect(() => {
        const fetchData = async () => {
            setLoading(true);
            try {
                const response = await fetch(url);
                if (!response.ok) {
                    throw new Error(`Error: ${response.status}`);
                }

                const result = await response.json();
                setData(result.data);
            } catch (err: any) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [url]);

    return { data, loading, error };
};

export default useUser;