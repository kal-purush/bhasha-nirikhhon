import { Component, OnInit } from '@angular/core';
import { NgxPermissionsService } from 'ngx-permissions';
import { Observable } from 'rxjs/Observable';
import { of } from 'rxjs/observable/of';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
})
export class AppComponent implements OnInit {
  title = 'app';
  ngOnInit(): void {
    this.makeReq().subscribe( (perm) => {
      console.log('perm', perm);
      this.permissionsService.loadPermissions(perm, (permissionName, permissionStore) => {
        return !!permissionStore[permissionName];
      });
    });
  }
  constructor(private permissionsService: NgxPermissionsService, private http: HttpClient) {

  }
  makeReq(): Observable<any> {
    //
    return this.http.get('/api/permissions');
  }
}