import { Injectable, Inject } from '@angular/core';

import { Request } from 'express';
import { REQUEST } from '@nguniversal/express-engine/tokens';

@Injectable()
export class RequestCookies {
    constructor(@Inject(REQUEST) private request: Request) {}

    get cookies() {
        return !!this.request.headers.cookie ? this.request.headers.cookie : null;
    }
}