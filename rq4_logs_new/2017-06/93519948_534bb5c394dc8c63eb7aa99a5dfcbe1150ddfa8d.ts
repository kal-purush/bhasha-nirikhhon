import { Injectable, Provider, Optional, SkipSelf } from '@angular/core';
import { UserToken } from './entity/user-token';

@Injectable()
export class AppContext {
    private _userToken: UserToken;

    get userToken(): UserToken {
        return this._userToken;
    }
}

export function APP_CONTEXT_PROVIDER_FACTORY(parentContext: AppContext) {
    return parentContext || new AppContext();
}

export const APP_CONTEXT_PROVIDER: Provider = {
    provide: AppContext,
    deps: [[new Optional(), new SkipSelf(), AppContext]],
    useFactory: APP_CONTEXT_PROVIDER_FACTORY
};