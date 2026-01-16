// eslint-disable-next-line @typescript-eslint/ban-types
export interface ApiResponse<T extends {}>{
    status: number;
    data?: T;
}

// eslint-disable-next-line @typescript-eslint/ban-types
export async function callApiService<T extends {}>(apiUrl: string): Promise<ApiResponse<T>> {
    const apiResponse: Response = await fetch(apiUrl);

    const {status} = apiResponse;

    if (status === 200) {
        const data: T = await apiResponse.json();

        return {
            status,
            data,
        };
    }
    return { status };
}