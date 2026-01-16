import { Injectable } from '@angular/core';
import { ToastrService, IndividualConfig } from 'ngx-toastr';
import { TranslateService } from '@ngx-translate/core';

@Injectable({
  providedIn: 'root'
})
export class GuiNotificatorService {
  private _options: any;
  private _title: string;

  constructor(private _toastr: ToastrService,
    private _translate: TranslateService) {
      this._options = {
        closeButton: true,
        enableHtml: true
      };
  }

  public showSuccess(message: string): void {
    this._title = this._translate.instant('toastr.success');
    this._toastr.success(message, this._title, this._options);
  }

  public showError(message: string): void {
    this._title = this._translate.instant('toastr.error');
    this._toastr.error(message, this._title, this._options);
  }

  public showWarning(message: string): void {
    this._title = this._translate.instant('toastr.warning');
    this._toastr.error(message, this._title, this._options);
  }

  public showInfo(message: string): void {
    this._title = this._translate.instant('toastr.information');
    this._toastr.info(message, this._title, this._options);
  }

}