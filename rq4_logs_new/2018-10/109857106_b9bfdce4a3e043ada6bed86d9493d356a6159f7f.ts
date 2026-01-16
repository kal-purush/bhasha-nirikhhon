import { Injectable, EventEmitter } from '@angular/core';

@Injectable({
    providedIn: 'root'
})
export class MensagemService {

    static mensagem: EventEmitter<any> = new EventEmitter();

    constructor() { }

    mostrarConfirmar() {

    }

}