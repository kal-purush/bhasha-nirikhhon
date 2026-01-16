import { Http, ConnectionBackend, Headers, RequestOptions, Request, Response, RequestOptionsArgs ,XHRBackend} from '@angular/http';
import {InterceptorService} from './interceptorservice';
export function httpfactory(backend: XHRBackend, options: RequestOptions) {
  return new InterceptorService(backend, options);
}