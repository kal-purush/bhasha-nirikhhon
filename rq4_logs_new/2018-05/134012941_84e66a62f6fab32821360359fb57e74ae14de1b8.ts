import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';
import { ListarTestesPage } from '../listar-testes/listar-testes';


@Component({
  selector: 'page-cadastrar-testes',
  templateUrl: 'cadastrar-testes.html'
})
export class CadastrarTestesPage {

  constructor(public navCtrl: NavController) {
  }
  goToListarTestes(params){
    if (!params) params = {};
    this.navCtrl.push(ListarTestesPage);
  }goToCadastrarTestes(params){
    if (!params) params = {};
    this.navCtrl.push(CadastrarTestesPage);
  }
}