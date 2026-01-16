import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class SyncService {

  private localApiUrl = 'http://localhost/olimpusGym/conf/sync.php';
  private remoteApiUrl = 'https://olympus.arvispace.com/olimpusGym/conf/sync.php';

  constructor(private http: HttpClient) {}

  getLocalUsers(): Observable<any> {
    return this.http.get(this.localApiUrl);
  }

  getRemoteUsers(): Observable<any> {
    return this.http.get(this.remoteApiUrl);
  }

  syncLocalUsers(users: any[]): Observable<any> {
    return this.http.post(this.localApiUrl, { usuarios: users });
  }

  syncRemoteUsers(users: any[]): Observable<any> {
    return this.http.post(this.remoteApiUrl, { usuarios: users });
  }
}