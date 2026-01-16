import { ItemCompra } from './../../../modelo/item-compra';
import { Compra } from './../../../modelo/compra';
import { Component } from '@angular/core';
import { NavController, NavParams, ViewController } from 'ionic-angular';

@Component({
  selector: 'page-modal-lista-item',
  templateUrl: 'modal-lista-item.html'
})
export class ModalListaItemPage {
  compra: Compra;
  itens: ItemCompra[] = [];

  constructor(public navParams: NavParams,
    public viewCtrl: ViewController) { }

  ionViewDidLoad() {
    console.log('ionViewDidLoad ModalListaItemPage');
    this.compra = this.navParams.get('compra');
    this.itens = this.compra.itens;
  }

  public dismiss(acao: string) {
    this.viewCtrl.dismiss();
  }

}