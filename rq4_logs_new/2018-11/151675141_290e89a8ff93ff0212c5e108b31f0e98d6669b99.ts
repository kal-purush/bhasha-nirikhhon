import { ErrorHandler } from '@angular/core';

export class GlobalErrorHandler implements ErrorHandler{

    handleError(error: any): void {
        throw error;
    }
}