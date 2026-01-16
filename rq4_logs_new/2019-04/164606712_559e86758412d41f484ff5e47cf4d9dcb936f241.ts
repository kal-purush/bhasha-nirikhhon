import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { tap } from 'rxjs/operators';
import { of as observableOf } from 'rxjs';

import { Department } from 'src/app/shared/interfaces/department';
import { environment } from 'src/environments/environment';

import { GenericListService } from 'src/app/shared/generics/generic-list-service/generic-list-service';
import { ListRequest } from 'src/app/shared/interfaces/list-request';

@Injectable({
  providedIn: 'root'
})
export class ProgramDepartmentService extends GenericListService {

  private cachedProgramDepartments: Map<number, Department> = new Map();

  constructor(
    protected readonly http: HttpClient,
  ) {
      super(http);
    }

  getProgramDepartments(req: ListRequest) {
    return this.list<Department>('departments', req).pipe(
      tap( data => {
        for(let i = 0; i < data.results.length; i++) {
          this.cachedProgramDepartments.set(data.results[i].id, data.results[i]);
        }
      }));
  }

  getProgramForm(id: number) {
    if (this.cachedProgramDepartments.has(id)) {
      return observableOf(this.cachedProgramDepartments.get(id));  //变成obserable对象
      
    }
    return this.http.get<Department>(`${environment.API_URL}/department/${id}/`);
  }

}