import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { MatSnackBar, MatSnackBarConfig} from '@angular/material/snack-bar'
@Injectable({
  providedIn: 'root'
})
export class NotificationService {

  constructor(public snackBar: MatSnackBar ) {}

  config: MatSnackBarConfig = {
    duration: 3000,
    horizontalPosition: 'center',
    verticalPosition: 'bottom'    
  }

  success(msg:string) {
    this.config['panelClass'] = ['success-snackbar']
    this.snackBar.open(msg,'', this.config)
  }

  error(msg:string) {
    this.config['panelClass'] = ['error-snackbar']
    this.snackBar.open(msg,'', this.config)
  }
}