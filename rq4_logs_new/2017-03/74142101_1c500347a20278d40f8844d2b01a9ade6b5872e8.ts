import { Injectable } from '@angular/core';
import {MdSnackBar, MdSnackBarConfig} from "@angular/material";

@Injectable()
export class NotificationService {

  constructor(public _snackBar: MdSnackBar) { }

  info(message: string) {
    console.log(message);

    this._display(message, 5000, null);
  }

  warn(message: string) {
    console.warn(message);

    this._display(message, 5000, ['warn']);
  }

  error(message: string) {
    console.error(message);

    this._display(message, 5000, ['error']);
  }

  _display(message: string, duration: number, extraClasses: [string]) {

    let config = new MdSnackBarConfig();
    config.duration = duration;
    config.extraClasses = extraClasses;

    this._snackBar.open(message, null, config);
  }
}