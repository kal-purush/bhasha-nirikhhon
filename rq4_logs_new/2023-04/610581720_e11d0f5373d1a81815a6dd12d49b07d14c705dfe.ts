/**
 * @author WMXPY
 * @namespace Proxy_Phrase_Portal
 * @description Put Phrase
 */

import { HTTP_RESPONSE_CODE } from "@sudoo/magic";
import { ERROR_CODE } from "../../../error/code";
import { panic } from "../../../error/panic";
import { fixURL } from "../../url";
import { LOCALE } from "@sudoo/locale";

export type PutPhraseProxyRequest = {

    readonly scopeDomain: string;
    readonly phraseIdentifier: string;
    readonly locale: LOCALE;
    readonly content: string;
};

export type PutPhraseProxyResponse = {

    // NOTHING
};

export const putPhraseProxy = async (
    phraseHost: string,
    request: PutPhraseProxyRequest,
): Promise<PutPhraseProxyResponse> => {

    const path: string = fixURL(`${phraseHost}/portal/phrase`);

    const response: Response = await fetch(path, {
        method: 'PUT',
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

    const jsonResponse: PutPhraseProxyResponse = await response.json();
    return jsonResponse;
};