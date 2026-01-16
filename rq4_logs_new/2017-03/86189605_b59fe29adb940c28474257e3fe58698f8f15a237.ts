import { ACOES_OPCAO_MERCADO } from './opcao-mercado';
import { Mercado } from './../../modelo/mercado';
import { BasePage } from './../base';
import { ItemCompra } from './../../modelo/item-compra';

import { NavParams, ViewController, Platform, ToastController, LoadingController } from 'ionic-angular';
import { Component, OnInit } from '@angular/core';
@Component({
    templateUrl: 'modal-opcao-mercado.html'
})
export class ModalOpcaoMercaoPage extends BasePage implements OnInit {
    acao: string;
    titulo: string;
    acoes: string[] = ACOES_OPCAO_MERCADO;
    item: Mercado;

    constructor(public platform: Platform,
        public params: NavParams,
        public viewCtrl: ViewController,
        protected loadingCtrl: LoadingController,
        protected toastCtrl: ToastController) {
        super(loadingCtrl, toastCtrl);
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