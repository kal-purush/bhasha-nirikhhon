import { Injectable } from '@angular/core';
import { SerializationHelper } from 'models/SerializationHelper';
import { EducfinamentAPIClient } from 'services/EducfinamentAPIClient';
import { environment } from 'environments/environment';
import { Organitzacio, Grup } from 'models/models';
import { Observable } from 'rxjs';

@Injectable()
export class OrganitzacioManagerAPIClient extends EducfinamentAPIClient {

    getOrganitzacio(id: number) {
        let url = environment.SERVER_API_URL + "/organitzacions/" + id;

        return Observable.create(observer => {
            this.getContentFromURL(url).subscribe(
                (res: Organitzacio) => {
                    observer.next(res);
                    observer.complete();
                }, err => {
                    observer.error(err);
                }
            );
        });

    }

}