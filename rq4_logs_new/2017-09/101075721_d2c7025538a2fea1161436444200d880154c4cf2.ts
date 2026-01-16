import { Injectable } from '@angular/core';
import { ApiServices } from '../shared/services/api.services';
import { Observable } from 'rxjs/Observable';
import { RequestOptions } from '@angular/http/http';
import { GlassRequest } from './glass/models/glassRequest.model';

@Injectable()
export class GlassesService {
  constructor(private apiServices: ApiServices) {}

  public getGlassById(id: number): Observable<any> {
      return this.apiServices.get('api/Products/GetById', id);
  }

  public getGlassesByVehicleInfo(makeId: number, modelId: number, bodyTypeId: number): Observable<any> {
      const header = { 'Content-Type': 'application/json' };

      const requestModel = new GlassRequest(makeId, modelId, bodyTypeId);
      return this.apiServices.post('api/Products/GetByVehicleInfo', requestModel, header);
  }
}
