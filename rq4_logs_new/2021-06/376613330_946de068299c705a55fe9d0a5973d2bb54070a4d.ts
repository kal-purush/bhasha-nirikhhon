import { useEffect, useState } from "react";

export type Fetcher<InType,OutType> = (input: InType) => Promise<OutType>;

const useFetcher = <InType,OutType>(fetcher: Fetcher<InType, OutType>, input: InType, forceReload?: unknown[]) : [OutType|null, boolean, string|null] => {
    const [data, setData] = useState<OutType|null>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string|null>(null);

    useEffect(() => {(async () => {
        setLoading(true);

        try {
            const newData = await fetcher(input);
            setData(newData);
            console.log("Data has been set");
            setLoading(false);
        } catch (e) {
            console.error(e);
            setError(e.message);
        }
    })();},forceReload ?? []);

    return [data, loading, error];
    
};

export default useFetcher;