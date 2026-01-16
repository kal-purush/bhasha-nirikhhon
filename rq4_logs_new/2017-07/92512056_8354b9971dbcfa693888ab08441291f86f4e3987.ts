import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';

import { EscolasPage } from '../escolas/escolas';
import { TurmasPage } from '../turmas/turmas';
import { AlunosPage } from '../alunos/alunos';
import { GruposPage } from '../grupos/grupos';

/**
 * Generated class for the CadastrosPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@IonicPage()
@Component({
  selector: 'page-cadastros',
  templateUrl: 'cadastros.html',
})
export class CadastrosPage {

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad CadastrosPage');
  }

  cadastroEscolas(){
    this.navCtrl.push(EscolasPage);
  }

  cadastroTurmas(){
    this.navCtrl.push(TurmasPage);
  }

  cadastroAlunos(){
    this.navCtrl.push(AlunosPage);
  }

  cadastroGrupos(){
    this.navCtrl.push(GruposPage);
  }
}