import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class MatchedVolunteersService {

  private apiUrl = 'http://localhost:3000/volunteers';

  constructor(private http: HttpClient) { }

  getMatchedVolunteers(opportunityId: string): Observable<any[]> {
    return this.http.get<any[]>(this.apiUrl).pipe(
      map(volunteers => 
        volunteers.filter(volunteer => 
          volunteer.opportunities_ids.includes(parseInt(opportunityId, 10))
        )
      )
    );
  }

}
