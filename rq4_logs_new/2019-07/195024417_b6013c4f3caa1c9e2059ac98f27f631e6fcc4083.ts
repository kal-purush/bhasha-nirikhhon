import { Injectable } from '@angular/core';
import { ToastrService } from 'ngx-toastr';

@Injectable({
  providedIn: 'root'
})
export class MessagesService {

  constructor(private toastrService: ToastrService) { }

  public handlerSuccess(message: string) {
    if (message) {
      this.toastrService.success(message);
    } else {
      this.toastrService.success('Success!');
    }
  }

  public handlerError(error: { message: string }) {
    if (error && error.message) {
      this.toastrService.error(error.message);
    } else {
      this.toastrService.error('Something went wrong! Please try again later');
    }
  }
}