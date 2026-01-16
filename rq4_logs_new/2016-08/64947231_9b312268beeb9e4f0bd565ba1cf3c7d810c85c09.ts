import {Injectable, provide} from '@angular/core';
import {BaseRequestOptions, RequestOptions} from '@angular/http'

@Injectable()
export class ExRequestOptions extends BaseRequestOptions  {
  constructor() {
    super();
    this.headers.append('X-CSRF-TOKEN', this.getCookie('CSRF-TOKEN'));
  }

  getCookie(name) {
    let value = "; " + document.cookie;
    let parts = value.split("; " + name + "=");
    if (parts.length == 2)
      return parts.pop().split(";").shift();
  }
}