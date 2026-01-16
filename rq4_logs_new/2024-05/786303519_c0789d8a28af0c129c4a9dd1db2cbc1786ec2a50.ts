import { requestHandler } from "./requestHandler";
import { API_BASE_URL } from "../config";
import { Snippet } from "@prisma/client";

const SNIPPETS_BASE_URL = API_BASE_URL + "/snippets";

type UserParams = {
    userId: number;
};

export const getSnippetsByUserId = requestHandler<UserParams, Snippet[]>(
    (params) => {
        let url = SNIPPETS_BASE_URL;
        if (params) {
            const searchParams = new URLSearchParams(
                params as unknown as string,
            );
            url = url + `?${searchParams}`;
        }
        return fetch(url);
    },
);