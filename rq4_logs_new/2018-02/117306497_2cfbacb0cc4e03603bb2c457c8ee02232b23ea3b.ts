import { Directive } from '@angular/core';
import { AsyncValidatorFn, AbstractControl, ValidationErrors } from '@angular/forms/';
import { HttpClient } from '@angular/common/http';
import { apiURL } from './url';
import { resolve } from 'path';


export function unusedValidator(http: HttpClient, type: string): AsyncValidatorFn {
  return (control: AbstractControl) => {
    if (!control.value)
      return new Promise((res) => res(null));
    return http.get(`${apiURL}/api/availability/${type}/${control.value}`).toPromise().then(
      (res: any) => {
        return res.availbile ? null : { 'availability': true };
      }
    );
  };
}