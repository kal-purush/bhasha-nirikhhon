import {alt} from "../../shared/alt";
import {FetchActions} from "../../shared/actions/FetchActions";

interface Actions {

    beginFetchToken(url: string): boolean;
    updateToken(token: string, url: string): any;
}

class OnetimeTokenActions extends FetchActions<string[]> implements Actions {

    beginFetchToken(url: string): boolean {
        return true;
    }

    updateToken(token: string): any {
        return token;
    }


}

export const onetimeTokenActions =
    alt.createActions<Actions>(OnetimeTokenActions);