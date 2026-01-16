import { Component } from '@angular/core';
import { IonicPage, NavController, AlertController, ModalController } from 'ionic-angular';

import { DbServiceProvider } from '../../providers/db-service/db-service';

import { ViewAvaliacoesModalPage } from '../view-avaliacoes-modal/view-avaliacoes-modal';

/**
 * Generated class for the ListAvaliacoesPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@IonicPage()
@Component({
  selector: 'page-list-avaliacoes',
  templateUrl: 'list-avaliacoes.html',
})
export class ListAvaliacoesPage {

  avaliacoes: any[] = [];

  constructor(public navCtrl: NavController,
              public dbService: DbServiceProvider,
              public alertCtrl: AlertController,
              public modalCtrl : ModalController) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad ListAvaliacoesPage');
    this.getAllAvaliacoes();
  }

  public openModalView(avaliacao, index){

    let obj = {id: avaliacao.id, nome: avaliacao.nome, date: avaliacao.date, index: index};
    var modalPage = this.modalCtrl.create(ViewAvaliacoesModalPage, obj);

    modalPage.present();
  }

  public getAllAvaliacoes(){
    this.dbService.getAllAvaliacoes()
      .then(avaliacoes => {
        console.log(avaliacoes);
        this.avaliacoes = avaliacoes;
      })
      .catch( error => {
        console.error( error );
      });
  }


}