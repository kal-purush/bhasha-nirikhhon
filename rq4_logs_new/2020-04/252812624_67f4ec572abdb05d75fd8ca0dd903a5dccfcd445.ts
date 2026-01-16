import { Injectable } from '@angular/core';
import { EducfinamentAPIClient } from 'services/EducfinamentAPIClient';
import { environment } from 'environments/environment';
import { Video } from 'models/models';
import { Observable } from 'rxjs';

@Injectable()
export class VideoManagerAPIClient extends EducfinamentAPIClient {

  obtenirVideo(id: number) {

    let url = environment.SERVER_API_URL + "/videos/" + id;

    return Observable.create(observer => {
        this.getContentFromURL(url).subscribe(
            (res: Video) => {
                observer.next(res);
                observer.complete();
            }, err => {
                observer.error(err);
            }
        );
    });

  }

  creaVideo(video: Video) {
    let url = environment.SERVER_API_URL + '/videos';

    return Observable.create(observer => {
      this.postContentToURL(url, JSON.stringify(video)).subscribe(
        (res: Video) => {
          observer.next(res);
          observer.complete();
        }, err => {
          observer.error(err);
        }
      )

    });

  }

}