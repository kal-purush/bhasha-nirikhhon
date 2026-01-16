import { useSearchParams } from "react-router-dom"

export const useFilters = <T extends Record<string, string | undefined>>() => {
    const [searchParams, setSearchParams] = useSearchParams();

    const setFilter = (key: string, value?: string | number) => {
        const newParams = new URLSearchParams(searchParams);

        if (value === undefined || value === '') {
            newParams.delete(key);
        } else {
            newParams.set(key, String(value));
        }

        setSearchParams(newParams);
    }

    const removeFilter = (key: string) => {
        const newParams = new URLSearchParams(searchParams);
        newParams.delete(key);
        setSearchParams(newParams)
    }

    const getFilters = () => {
        const filters: Record<string, string> = {};

        for (let [key, value] of searchParams.entries()) {
            filters[key] = value;
        }

        return filters as T;
    }

    const clearFilter = () => setSearchParams({});

    return { setFilter, removeFilter, filters: getFilters(), clearFilter }
}