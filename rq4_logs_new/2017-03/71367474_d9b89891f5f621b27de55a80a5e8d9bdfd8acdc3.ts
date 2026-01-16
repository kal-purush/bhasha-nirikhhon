import { Injectable } from '@angular/core';
import { Http, Response, Headers, RequestOptions } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import { environment } from '../../environments/environment';
import { Feature } from './feature';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

@Injectable()
export class SeeyondService {
  apiUrl = 'https://' + environment.API_URL + '/seeyonds/';

  constructor(
    private http: Http,
    private feature: Feature
  ) { }

  getMyFeatures(uid: number) {
    return this.http.get(this.apiUrl + 'list/' + uid)
      .map((res: Response) => res.json())
      .catch(this.handleError)
  }

  loadFeature(id: number) {
    console.log('service loading feature: ' + id);
    console.log(this.apiUrl + id);
    return this.http.get(this.apiUrl + id)
      .catch(this.handleError);
  }

  updateFeature(id: number, uid: number) {
    let patchData = {
      "id": id,
      "uid": uid,
      "feature_type": this.feature.type,
      "title": this.feature.title,
      "name": this.feature.name,
      "width": this.feature.width,
      "height": this.feature.height,
      "radius": this.feature.radius,
      "angle": this.feature.angle,
      "ceiling_length": this.feature.ceilingLength,
      "tessellation": this.feature.tessellation,
      "pattern_strength": this.feature.patternStrength,
      "material": this.feature.material,
      "boxsize": this.feature.boxsize,
      "boxes": this.feature.boxes,
      "xml": this.feature.xml
    }

    let headers = new Headers({"Content-Type": "application/json"});
    let options = new RequestOptions({headers: headers});

    return this.http.patch(this.apiUrl + id, patchData, options)
      .map((res: Response) => {
        return res.json() || {}
      })
      .catch(this.handleError);
  }

  deleteFeature(id: number, uid: number) {
    return this.http.delete(this.apiUrl)
  }

  private handleError(error: any) {
    let errorJson = error.json();
    if (errorJson) {
      return Observable.throw(errorJson.message || 'Server Error');
    }

    return Observable.throw('Unknown Error');
  }

}