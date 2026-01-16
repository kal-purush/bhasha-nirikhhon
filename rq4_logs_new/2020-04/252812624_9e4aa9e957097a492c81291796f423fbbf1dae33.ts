import { Injectable } from '@angular/core';
import { EducfinamentAPIClient } from 'services/EducfinamentAPIClient';
import { environment } from 'environments/environment';
import { Activitat, User, AcceptaActivitatRequest, Video } from 'models/models';
import { Observable } from 'rxjs';

@Injectable()
export class VideoManagerAPIClient extends EducfinamentAPIClient {

  modificaVideo(video: Video) {
    
    let url = environment.SERVER_API_URL + '/videos/' + video.id;
    return Observable.create(observer => {
      this.putContentToURL(url, JSON.stringify(video)).subscribe(
        (res: Activitat) => {
          observer.next(res);
          observer.complete();
        }, err => {
          observer.error(err);
        }
      );
    });

  }

}