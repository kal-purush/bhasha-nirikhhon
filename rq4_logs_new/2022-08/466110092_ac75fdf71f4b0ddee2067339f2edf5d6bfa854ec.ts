import {Component} from '@angular/core';
import {HcertService} from '../../service/hcert.service';
import {ErrorHandlerService} from '../../service/error-handler.service';
import {ClientCommunication} from '../../server/clientCommunication';

@Component({
  selector: 'app-base45-decode',
  templateUrl: './base45-decode.component.html'
})
export class Base45DecodeComponent {
  base45ServerResponse: string | undefined;

  constructor(private hcertService: HcertService, public errorHandlerService: ErrorHandlerService) {}

  decodeBase45(stringInput: string): void {
    if (stringInput) {
      const base45DecodeServerRequest: ClientCommunication.Base45DecodeServerRequest = {
        base45Decode: stringInput
      };
      this.hcertService.decodeBase45(base45DecodeServerRequest).subscribe({
        error: err => {
          this.errorHandlerService.cleanupErrors();
          this.errorHandlerService.setErrors(err);
          this.base45ServerResponse = '';
        },
        next: res => {
          this.errorHandlerService.cleanupErrors();
          this.base45ServerResponse = res;
        }
      });
    }
  }
}