import axios from "axios";
import { envKeys } from "@/enums/env-keys";
import { getStringEnv } from "@/helpers/get-string-env";
import { ApiEndpointName } from "@/store/api-endpoint-names";

export const apiUrl = getStringEnv(envKeys.apiUrl);

export async function getAll<T>(endpoint: ApiEndpointName) {
    const { data } = await axios.get<T[]>(`${apiUrl}${endpoint}`);

    return data;
}

export async function createOne<T>(endpoint: string, body: T) {
    const { data } = await axios.post<T>(`${apiUrl}${endpoint}`, body);

    return data;
}

export async function getOne<T>(endpoint: string, id: string) {
    const { data } = await axios.get<T>(`${apiUrl}${endpoint}/${id}`);

    return data;
}

export async function deleteOne<T>(endpoint: string, id: string) {
    const { data } = await axios.delete<T>(`${apiUrl}${endpoint}/${id}`);

    return data;
}

export async function updateOne<T>(endpoint: string, id: string, body: Partial<T>) {
    const { data } = await axios.patch<T[]>(`${apiUrl}${endpoint}/${id}`, body);

    return data;
}