import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams,AlertController  } from 'ionic-angular';
import { AgendamentoDao } from '../../domain/agendamento/agendamento-dao'
import { Agendamento } from '../../domain/agendamento/agendamento'
import { AgendamentoService } from '../../domain/agendamento/agendamento-service'  

@Component({ 
  selector:"page-agendamentos",
  templateUrl: 'agendamentos.html',
})
export class AgendamentosPage {

  public agendamentos : Agendamento[];

  constructor(
      public navCtrl: NavController,
      public navParams: NavParams,
      private _dao: AgendamentoDao,
      private _service: AgendamentoService,
      private _alertCtrl:AlertController,
      
    ) {
  } 

  ionViewDidLoad() {
 
    this._dao
      .listaTodos().then(agendamentos => this.agendamentos = agendamentos)

    console.log('ionViewDidLoad AgendamentosPage');
  }

  reenvia(agendamento:Agendamento){
    this._service
    .reagenda(agendamento).then(confirmado =>{

      agendamento ?
      this._alertCtrl.create({
        title:'Aviso',
        subTitle:'Agendamento sucesso',
        buttons:[{
          text:'Ok', 
          handler: () => { 
            
          }
        }]
      }).present()
      
      :
      this._alertCtrl.create({
        title:'Aviso',
        subTitle:'Nao foi possivel',
        buttons:[{
          text:'Ok', 
          handler: () => { 
             
          }
        }]
      }).present()
    })
  }
}