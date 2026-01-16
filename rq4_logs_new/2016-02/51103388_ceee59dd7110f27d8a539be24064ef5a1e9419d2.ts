import {Injectable} from 'angular2/core';
import {TokenService} from './token-service'
import {Headers} from 'angular2/http';
import {StringUtils} from '../util/string-utils';

@Injectable()
export class HeaderService extends Headers {

    constructor(private tokenService: TokenService) {
        super();
    }

    authenticatedHeader(): Headers {
        if (this.tokenService.hasToken()) {
            this.append("X-Auth-Token", this.tokenService.getToken());
        }
        return this;
    }

    authenticationHeaders(username: string, password: string): Headers {
        this.append("X-Auth-Username", username);
        this.append("X-Auth-Password", password);
        return this;
    }
}