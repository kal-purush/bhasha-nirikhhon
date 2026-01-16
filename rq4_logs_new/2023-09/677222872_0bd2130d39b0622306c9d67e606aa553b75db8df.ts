import fetch from "unfetch";
import { GenericRecord, IHttpConfig } from "./common";
import { InvalidResponseError } from "../../utils/errors";

export namespace Network {
    /**
     * Performs a request and returns a JSON object with the response
     */

    export async function get(config: IHttpConfig, path: string, data?: GenericRecord) {
        const { url, headers } = config;
        const endpoint: URL = new URL(path, url);
        for (const [key, value] of Object.entries(data ?? {})) {
            if (value != null) {
                endpoint.searchParams.set(key, String(value));
            }
        }
        const response = await fetch(endpoint.href, {
            method: "GET",
            headers
        });
        if (!response.ok) {
            throw new InvalidResponseError(response);
        }
        return response.json();
    }

    export async function post(config: IHttpConfig, path: string, data?: any) {
        const { url, headers } = config;
        const endpoint: URL = new URL(path, url);
        const response = await fetch(endpoint.href, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                ...headers
            },
            body: JSON.stringify(data)
        });

        if (!response.ok) {
            throw new InvalidResponseError(response);
        }
        return response.json();
    }
}