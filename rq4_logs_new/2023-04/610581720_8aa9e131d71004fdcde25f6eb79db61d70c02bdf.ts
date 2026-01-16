/**
 * @author WMXPY
 * @namespace Proxy_Phrase_Portal
 * @description Post Query Phrases
 */

import { HTTP_RESPONSE_CODE } from "@sudoo/magic";
import { ERROR_CODE } from "../../../error/code";
import { panic } from "../../../error/panic";
import { fixURL } from "../../url";

export type PostQueryPhrasesProxyRequest = {

    readonly scopeDomain: string;
    readonly searchPhraseIdentifier?: string;
    readonly page?: number;
};

export type PostQueryPhrasesProxyResponse = {

    // NOTHING
};

export const postQueryPhrasesProxy = async (
    phraseHost: string,
    request: PostQueryPhrasesProxyRequest,
): Promise<PostQueryPhrasesProxyResponse> => {

    const path: string = fixURL(`${phraseHost}/portal/phrase/query`);

    const response: Response = await fetch(path, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
    });

    if (response.status !== HTTP_RESPONSE_CODE.OK) {
        throw panic.code(
            ERROR_CODE.REQUEST_FAILED_1,
            await response.json(),
        );
    }

    const jsonResponse: PostQueryPhrasesProxyResponse = await response.json();
    return jsonResponse;
};