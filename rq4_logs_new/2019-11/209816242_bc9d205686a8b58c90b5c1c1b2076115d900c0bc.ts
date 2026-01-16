/**
 * @author Andreas Gøricke, s153804
 */

import { Injectable } from '@angular/core';
import { ScadadataScores } from '../models/sensors/scadadata-scores.model';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { BaseService } from './base.service';
import { saveAs } from 'file-saver';


@Injectable({
  providedIn: 'root'
})

export class ScadadataService extends BaseService {

  constructor(http: HttpClient) {
    super(http);
  }

  getWeightedScore() {
    return this.http.get<number>(`${this.backendBaseUrl}/api/v1/scadadata/getweightedscore`);
  }

 
}