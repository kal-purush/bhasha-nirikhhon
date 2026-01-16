import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiService } from './api.service';

@Injectable({
  providedIn: 'root',
})
export class SharedService {
  constructor(private api: ApiService) {}

  public getNumElementi(root: string): Observable<any> {
    console.log(root + '   elemnts');
    return this.api.get(root + '/count', '');
  }

  public getElementiPagination(
    root: string,
    index: number,
    size: number
  ): Observable<any> {
    return this.api.get(root + '/page?page=' + index + '&size=' + size, '');
  }
}