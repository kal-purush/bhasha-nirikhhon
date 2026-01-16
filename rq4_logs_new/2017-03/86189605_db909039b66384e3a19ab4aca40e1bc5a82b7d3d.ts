import { Component } from '@angular/core';
import { ViewController } from 'ionic-angular';

export const ACOES_LISTA = ['deleteAll'];

@Component({
    selector: 'opcao-lista',
    templateUrl: 'opcao-lista.html'
})
export class OpcaoListaPage {
    acao = ACOES_LISTA[0];

    constructor(private viewCtrl: ViewController) { }

    onAction(action: string) {
        this.viewCtrl.dismiss({
            action: action
        });
    }

}