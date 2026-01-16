import { baseAxios } from "@/utils/baseAxios";
import { Report } from "@/utils/types";
import { useQuery } from '@tanstack/react-query';

export const fetchReportById = async ({ id }: { id: string }) => {
    const { data } = await baseAxios.get<Report>(`/reports/${id}`);
    return data
}

export const useFetchReportById = ({ id }: { id: string }, options: { enabled?: boolean } = {}) => {
    const query = useQuery({
        queryFn: () => fetchReportById({ id }),
        queryKey: ["reports", { id }],
        ...options
    })

    return [query.data, query] as const
}