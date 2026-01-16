import { Injectable } from '@angular/core';
import { Store } from '@ngrx/store';
import { IAppState } from '../Store/Reducers/index';
import { CustomHttp } from './customHttp';
import { FormTemplateActions } from '../Store/Actions/form-template.actions';
import { Observable } from 'rxjs';
import { BASE_URL } from '../Constants/settings';
import { FormTemplate } from '../Store/Models/form-template';

@Injectable()
export class FormTemplateService {

  constructor(private store: Store<IAppState>, private chttp: CustomHttp) {}

  public getAll(): void {
    this.store.dispatch(FormTemplateActions.getAll());
  }

  public getAll$(): Observable<Response> {
    return this.chttp.get(`${BASE_URL}/form_templates`)
      .map(res => res.json())
      .map(res => res.form_templates)
      .do(templates => this.store.dispatch(FormTemplateActions.getAllSuccess(templates)))
      .catch(err => {
        this.store.dispatch(FormTemplateActions.getAllFail(err));
        return Observable.throw(err);
      })
  }

  public create(new_form: FormTemplate): void {
    this.store.dispatch(FormTemplateActions.create(new_form));
  }

  public create$(new_form: FormTemplate): Observable<Response> {
    let json = JSON.stringify(new_form);

    return this.chttp.post(`${BASE_URL}/form_templates`, json)
      .map(res => res.json())
      .map(json => json.form_template)
      .do(template => this.store.dispatch(FormTemplateActions.createSuccess(template)))
      .catch(err => {
        this.store.dispatch(FormTemplateActions.createFail(err));
        return Observable.throw(err);
      })
  }
}