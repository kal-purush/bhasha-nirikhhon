/**
 * Created by caiguoqing on 2017/5/23.
 */

import { RequestOptions , Http } from '@angular/http';
import { HttpInterceptorBackend } from './http-interceptor-backend';

export function httpFactory(httpInterceptorBackend:HttpInterceptorBackend,requestOptions:RequestOptions):Http{
  return new Http(httpInterceptorBackend,requestOptions);
}