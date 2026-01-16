import { Injectable } from '@angular/core'
import { HttpClient, HttpHeaders } from '@angular/common/http'
import { Observable, of } from 'rxjs'
import { catchError } from 'rxjs/operators'
import { Memo } from '@app/shared'

@Injectable({
  providedIn: 'root'
})
export class MemoService {
  private httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
  }

  constructor(private http: HttpClient) {}

  addMemo(memo: Memo): Observable<Memo> {
    return this.http
      .post<Memo>('http://localhost:3000/memos', memo, this.httpOptions)
      .pipe(catchError(() => of(null as Memo)))
    // .pipe(catchError(err => throwError(err))
  }
}