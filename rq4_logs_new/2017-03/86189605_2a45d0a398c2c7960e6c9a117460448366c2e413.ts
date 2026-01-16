import { ItemCompra } from './../../modelo/item-compra';
import { ACOES } from './opcao-item-compra';

import { NavParams, ViewController, Platform } from 'ionic-angular';
import { Component, OnInit } from '@angular/core';
@Component({
    templateUrl: 'modal-opcao-item-compra.html'
})
export class ModalOpcaoItemCompraPage implements OnInit {
    acao: string;
    titulo: string;
    acoes: string[] = ACOES;
    item: ItemCompra;

    constructor(public platform: Platform,
        public params: NavParams,
        public viewCtrl: ViewController) {
    }

    ngOnInit() {
        this.item = this.params.get('item');
        this.acao = this.params.get('acao');
        this.titulo = this.acao;
    }

    public onSubmit(event: any): void {
        event.preventDefault();
        this.dismiss(this.acao);

    }

    public dismiss(acao: string) {
        this.viewCtrl.dismiss({
            acao: acao,
            item: this.item
        });
    }

}