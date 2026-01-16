import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Http, Response, RequestOptions, Headers, RequestMethod } from '@angular/http';

import 'rxjs/add/operator/map';

import { RestClientService } from '../../_service/rest-client.service';
import { Skill } from '../../_model/skill.model';
import { URL_BASE, URL_SKILL_LIST, URL_SKILL_ADD, URL_SKILL_DELETE } from '../../../environments/rest-environment';

@Injectable()
export class SkillService {

    constructor(private _restClientService: RestClientService) { }

    public list(): Observable<Skill[]> {
        const obs: Observable<Response> = this._restClientService.list(URL_BASE, URL_SKILL_LIST);
        obs.subscribe(rs => console.log(rs));
        return obs.map((res: Response) => <Skill[]>res.json());
    }

    insert(skill: Skill): Observable<Response> {
        return this._restClientService.insert(
            skill,
            URL_BASE, URL_SKILL_ADD
        );
    }


    delete(skill: Skill): Observable<Response> {
        return this._restClientService.delete(
            skill,
            URL_BASE, URL_SKILL_DELETE,
            // ).map( (res: Response) => res.text() );
        );
    }


}