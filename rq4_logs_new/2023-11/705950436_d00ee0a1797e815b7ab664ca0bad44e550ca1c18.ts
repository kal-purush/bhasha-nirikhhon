import { Injectable } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';

@Injectable({
    providedIn: 'root'
})
export class ToastService {

    constructor(
        private _matSnackBar: MatSnackBar
    ) { }

    /**
     * Success toastr
     * 
     * @param message 
     * @param status 
     */
    successMessage(message: string, status: any) {
        this._matSnackBar.open(message, status, {
            duration: 4000,
            verticalPosition: 'top',
            horizontalPosition: 'center',
        });
    }

    /**
    * Error toastr
    * 
    * @param message 
    * @param status 
    */
    errorMessage(message: string, status: any) {
        this._matSnackBar.open(message, status, {
            duration: 4000,
            verticalPosition: 'top',
            horizontalPosition: 'center',
        });
    }
}