import {Injectable} from '@angular/core';
import {HttpClientService} from '../../frontend/services/http-client.service';
import {FileImage, IFileImage} from '../../frontend/models/file-image.model';
import {AppUris} from '../../../globals/app-uris';
import {Response} from '@angular/http';

@Injectable()
export class DataService {

  constructor(private _http: HttpClientService) {
  }

  getImageData(uri: string): Promise<FileImage> {
    return this._http.get(AppUris.ImageDataGet(uri))
      .then((response: Response) => {
        const json = response.json();
        const spec = <IFileImage>json;
        return new FileImage(spec);
      })
      .catch((response: Response) => console.error(response));
  }

}