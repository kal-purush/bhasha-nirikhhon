import { Injectable } from '@angular/core';

@Injectable()
export class ErrorService {

    message(error) {
        switch (error.status) {
            case 403:
                return 'Incorrect username or password. Please try again'
            case 409:
                return 'This username already exists. Please try again.'
            // case 500:
            //     return 'Da backend betta get ther ack together.'
            default:
                return `Something went wrong. Report the following: "${error.status} (${error.error}): ${error.message} (Timestamp: ${error.timestamp})"`
        }
    }

}