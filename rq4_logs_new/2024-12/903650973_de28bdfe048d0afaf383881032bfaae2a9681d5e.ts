import React from 'react';

interface APIDoc {
    id: number;
    apiId: number;
    documentUrl: string;
    logoUrl: string;
    codeExamples: string;
    status: string;
}

interface API {
    id: number;
    onwerId: number;
    name: string;
    description: string;
    category: string;
    pricingUrl: string;
    basePrice: number;
    status: string;
    overallSubscription: number;
    documentations: APIDoc[];
    reviews: Review[];
}

interface Review {
    reviewId: number;
    userId: number;
    apiId: number;
    rating: number;
    comment: string;
    reviewDate: Date;
}

const useApis = () => {
    const [data, setData] = React.useState<API[] | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState(null);

    React.useEffect(() => {
        const fetchApis = async () => {
            try {
                const response = await fetch(`https://localhost:7036/api/api`);

                if (!response.ok) {
                    throw new Error(`Error: ${response.status}`);
                }

                const result = await response.json();

                // Extract the $values from the response
                const extractedData = result.data.$values.map((api: any) => ({
                    ...api,
                    documentations: api.documentations?.$values || [],
                }));

                setData(extractedData);
            } catch (err: any) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchApis();
    }, []);

    return { data, loading, error };
};

export default useApis;