import {HttpResponseErrorHandler} from './http-response-error-handler';
import {HttpErrorResponse} from '@angular/common/http';
import {Injectable} from '@angular/core';
import {ToastService} from '../services/toast.service';

@Injectable()
export class AuthenticationErrorHandler implements HttpResponseErrorHandler {

  constructor(private toastService: ToastService) {}

  matches(error: HttpErrorResponse): boolean {
    return error.status === 401;
  }

  handle(error: HttpErrorResponse) {
    this.toastService.addMessage('errors.login.credentials.wrong', 'danger', 5000);
  }

}