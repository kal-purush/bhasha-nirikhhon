import { Injectable } from '@angular/core';
import { EducfinamentAPIClient } from 'services/EducfinamentAPIClient';
import { environment } from 'environments/environment';
import { Activitat, Grup } from 'models/models';
import { Observable } from 'rxjs';

@Injectable()
export class GrupManagerAPIClient extends EducfinamentAPIClient {

  creaGrup(grup: Grup) {
    let url = environment.SERVER_API_URL + '/grups';

    return Observable.create(observer => {
      this.postContentToURL(url, JSON.stringify(grup)).subscribe(
        (res: Grup) => {
          observer.next(res);
          observer.complete();
        }, err => {
          observer.error(err);
        }
      )
    });

  }

  

}