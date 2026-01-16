import { alt } from "../../shared/alt";
import { FetchActions } from "../../shared/actions/FetchActions";
import { decodeOneTimeToken, OneTimeToken } from "../models/OneTimeToken";

interface Actions {
    beginFetchToken(): boolean;
    receiveToken(token: string): OneTimeToken;
}

class OneTimeTokenActions extends FetchActions<string[]> implements Actions {
    beginFetchToken(): boolean {
        return true;
    }

    receiveToken(token: string): OneTimeToken {
        return decodeOneTimeToken(token);
    }
}

export const oneTimeTokenActions =
    alt.createActions<Actions>(OneTimeTokenActions);