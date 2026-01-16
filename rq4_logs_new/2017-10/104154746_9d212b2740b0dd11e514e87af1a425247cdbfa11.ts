import { Injectable } from '@angular/core';

import { Response } from '@angular/http';
import { URL_MAIL_SEND, URL_BASE } from '../../../environments/rest-environment';
import { RestClientService } from '../../_service/rest-client.service';
import { Observable } from 'rxjs/Observable';
import { Mail } from '../../_model/mail.model';

@Injectable()
export class MailService {

  constructor(private _restClientService: RestClientService) { }

  send(mail: Mail): Observable<Response> {
    return this._restClientService.insert(
      mail,
      URL_BASE, URL_MAIL_SEND
    );
  }
}