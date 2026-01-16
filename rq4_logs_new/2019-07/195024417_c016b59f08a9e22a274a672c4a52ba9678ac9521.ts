import { Injectable } from '@angular/core';
import { ToastrService } from 'ngx-toastr';

@Injectable({
  providedIn: 'root'
})
export class HandlerToastrService {

  constructor(private toaster: ToastrService) { }

  handlerSuccess(message: string) {
    if (message) {
      this.toaster.success(message);
    } else {
      this.toaster.success('Successfully!');
    }
  }

  handlerError(error: { message: string }) {
    if (error && error.message) {
      this.toaster.error(error.message);
    } else {
      this.toaster.error('Error!');
    }
  }
}