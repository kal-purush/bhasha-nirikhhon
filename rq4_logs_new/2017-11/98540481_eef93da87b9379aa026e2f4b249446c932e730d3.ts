import { Injectable } from '@angular/core';
import {HttpClient, HttpParams, HttpHeaders} from '@angular/common/http';
import { RequestOptions, Headers } from '@angular/http';
import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';

import {environment} from '../../../environments/environment';

import {AvatarInterface} from '../model/avatar.interface';
import {JobInterface} from '../model/job.interface';
import {StatusInterface} from '../model/status.interface';
import {ProfileInterface} from '../model/profile.interface';
import {GoalTypeInterface} from '../model/goal-type.interface';
import {AvatarSelectionParamsInterface} from '../model/avatar-selection-params.interface';

import {response} from './backend-grafici-response-data';

@Injectable()
export class BackendHttpService {
  private apiurl = environment.apiurl;

  constructor(private http: HttpClient) { }

  getAllAvatars() {
    const url = this.apiurl + 'avatarlist';
    return this.http.get<Array<AvatarInterface>>(url)
                      .map(data => data['results']);
  }
  getAvatarsForProfile(params: AvatarSelectionParamsInterface) {
    const url = this.apiurl + 'avatars4profile';
    return this.http.post<Array<AvatarInterface>>(url, params)
                      .map(data => data['results']);
  }

  getJobList() {
    const url = this.apiurl + 'joblist';
    return this.http.get<Array<JobInterface>>(url)
                      .map(data => data['results']);
  }

  getStatusList() {
    const url = this.apiurl + 'statuslist';
    return this.http.get<Array<StatusInterface>>(url)
                      .map(data => data['results']);
  }

  saveProfile(profile: ProfileInterface) {
    const url = this.apiurl + 'saveprofile';
    return this.http.put(url, profile)
                      .map(data => {
                        const profileId = data['results'];
                        profile.id = profileId;
                        return profileId;
                      });
  }
  getProfile(id: string) {
    const params = new HttpParams().set('id', id);
    const url = this.apiurl + 'getprofile';
    return this.http.get(url, {params})
                      .map(data => data['results']);
  }
  getAllProfiles() {
    const url = this.apiurl + 'getallprofiles';
    return this.http.get(url)
                      .map(data => data['results']);
  }

  getGoalTypeList() {
    const url = this.apiurl + 'goaltypelist';
    return this.http.get<Array<GoalTypeInterface>>(url)
                      .map(data => data['results']);
  }

  getProjection(profile: ProfileInterface) {
    // const url = this.apiurl + 'projection';
    for (const goal of profile.goals) {
      goal.type = {... goal.type};
      delete goal.type.value;
    }
    if (!profile.valueAtRisk) {
      profile.valueAtRisk = 500;
    }
    if (!profile.investmentThreashold) {
      profile.investmentThreashold = 5000;
    }
    const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
    headers.append('Accept', 'application/json');
    // const options = new RequestOptions({headers: headers});
    const url = 'http://www.breakindatacompany.com/RobotWS/robotws/roboservice';
    return this.http.post(url, profile, {headers: headers});
  }

  getProjectionsData(profile: ProfileInterface) {
    return Observable.of(response);
  }

}