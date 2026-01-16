import { Injectable } from '@angular/core';
import { TokenManager } from './token-manager.service';
import { PageRefresh } from '../pages/services/page-refresh.service';

@Injectable()
export class Logout {

    constructor(protected _tokenManager: TokenManager, protected _pageRefresh: PageRefresh) {}

    public logout(): void {
        this._tokenManager.removeToken();
        this._pageRefresh.reset();
        location.reload();
    }
}