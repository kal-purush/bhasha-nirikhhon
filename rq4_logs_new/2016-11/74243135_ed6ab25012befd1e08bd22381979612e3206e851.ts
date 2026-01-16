import { Http } from '@angular/http';
import { Headers, RequestOptions, RequestOptionsArgs, Response } from '@angular/http';

import { Observable } from 'rxjs/Observable';

export class SecuredHttp extends Http
{
    authorizationToken: string = "";

    request(url: string, options?: RequestOptionsArgs): Observable<Response>
    {
        if (options)
        {
            options.headers.append('Authorization', this.authorizationToken);
        } else
        {
            let headers = new Headers({ 'Authorization': this.authorizationToken });
            let options = new RequestOptions({ headers: headers });
        }

        return super.request(url, options);
    };

}