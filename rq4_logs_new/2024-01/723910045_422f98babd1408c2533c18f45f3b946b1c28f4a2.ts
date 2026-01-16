import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, map } from 'rxjs';
import { BaseHttpService } from '../../base-http.service';

export interface Sdata {
  systemName: string;
  logoFileHash: string;
  themeColor: string;
}

@Injectable({
  providedIn: 'root'
})
export class StyleService {

  constructor(public http: BaseHttpService, private https: HttpClient) { }
  public submit(params: Sdata): Observable<any> {
    return this.http.post(`/v1/fxsp/sys/style/manage/submit`, params);
  }

  public search(): Observable<any> {
    return this.http.post(`/v1/fxsp/anon/common/system/style/search`, {});
  }
}