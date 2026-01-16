import { Injectable } from '@angular/core';

@Injectable({
    providedIn: 'root'
})
export class MessageService {

    constructor() { }

    public success(message, title = 'Sucesso') {
        console.log(title, message);
    }

    public info(message, title = 'Info') {
        console.log(title, message);
    }

    public warning(message, title = 'Atenção') {
        console.log(title, message);
    }

    public error(message, title = 'Erro') {
        console.log(title, message);
    }
}