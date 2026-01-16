import { Component, ViewChild } from '@angular/core';
import { IonicPage, NavController, NavParams, ViewController, ModalController } from 'ionic-angular';
import { Chart, ElementRef } from 'chart.js';

import { DbServiceProvider } from '../../providers/db-service/db-service';
/**
 * Generated class for the ViewAvaliacoesGraficoPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@IonicPage()
@Component({
  selector: 'page-view-avaliacoes-grafico',
  templateUrl: 'view-avaliacoes-grafico.html',
})
export class ViewAvaliacoesGraficoPage {

  aluno1 = {
    alunoNome: this.navParams.data[0].alunoNome,
    funcaoNome: this.navParams.data[0].funcaoNome,
    funcao: this.navParams.data[0].funcao
  }

  aluno2 = {
    alunoNome: this.navParams.data[1].alunoNome,
    funcaoNome: this.navParams.data[1].funcaoNome,
    funcao: this.navParams.data[1].funcao
  }

  aluno3 = {
    alunoNome: this.navParams.data[2].alunoNome,
    funcaoNome: this.navParams.data[2].funcaoNome,
    funcao: this.navParams.data[2].funcao
  }

  aluno4 = {
    alunoNome: this.navParams.data[3].alunoNome,
    funcaoNome: this.navParams.data[3].funcaoNome,
    funcao: this.navParams.data[3].funcao
  }

  constructor(public navCtrl: NavController,
              public navParams: NavParams,
              public dbService: DbServiceProvider,
              public viewCtrl : ViewController,
              public modalCtrl : ModalController) {
  }

  public closeModal(){
    this.viewCtrl.dismiss();
  }

  @ViewChild('barCanvas') barCanvas: ElementRef;
  @ViewChild('barCanvasOrgan') barCanvasOrgan: ElementRef;
  @ViewChild('barCanvasProg') barCanvasProg: ElementRef;
  @ViewChild('barCanvasLider') barCanvasLider: ElementRef;
  barChart: any;
  barChartOrgan: any;
  barChartProg: any;
  barChartLider: any;


  ionViewDidLoad() {
    this.graficos();
  }

  respConstrutor = {
    respNao: 0,
    respInsuficiente: 0,
    respParcialmente: 0,
    respSim: 0
  };

  respOrganizador = {
    respNao: 0,
    respInsuficiente: 0,
    respParcialmente: 0,
    respSim: 0
  };

  respProgramador = {
    respNao: 0,
    respInsuficiente: 0,
    respParcialmente: 0,
    respSim: 0
  };

  respLider = {
    respNao: 0,
    respInsuficiente: 0,
    respParcialmente: 0,
    respSim: 0
  };

verificaRepostas(respostas: any[], tipoAluno: number){
  let respNaoCount = 0;
  let respInsuficienteCount = 0;
  let respParcialmenteCount = 0;
  let respSimCount = 0;

  for (let index = 0; index < respostas.length; index++) {
    if(respostas[index] == 'nao'){
      respNaoCount += 1;
    } else if(respostas[index] == 'insuficiente'){
      respInsuficienteCount += 1;
    } else if(respostas[index] == 'parcialmente'){
      respParcialmenteCount += 1;
    } else if(respostas[index] == 'sim'){
      respSimCount += 1;
    }
  }

  if(tipoAluno == 1){
    this.respConstrutor.respNao = respNaoCount;
    this.respConstrutor.respInsuficiente = respInsuficienteCount;
    this.respConstrutor.respParcialmente = respParcialmenteCount;
    this.respConstrutor.respSim = respSimCount;
  } else if(tipoAluno == 2){
    this.respOrganizador.respNao = respNaoCount;
    this.respOrganizador.respInsuficiente = respInsuficienteCount;
    this.respOrganizador.respParcialmente = respParcialmenteCount;
    this.respOrganizador.respSim = respSimCount;
  } else if(tipoAluno == 3){
    this.respProgramador.respNao = respNaoCount;
    this.respProgramador.respInsuficiente = respInsuficienteCount;
    this.respProgramador.respParcialmente = respParcialmenteCount;
    this.respProgramador.respSim = respSimCount;
  } else if(tipoAluno == 4){
    this.respLider.respNao = respNaoCount;
    this.respLider.respInsuficiente = respInsuficienteCount;
    this.respLider.respParcialmente = respParcialmenteCount;
    this.respLider.respSim = respSimCount;
  }

}

graficoConstrutor(){
  let respostasConstrutor: any[] = [];
  respostasConstrutor.push(this.navParams.data[0].resposta1);
  respostasConstrutor.push(this.navParams.data[0].resposta2);
  respostasConstrutor.push(this.navParams.data[0].resposta3);
  respostasConstrutor.push(this.navParams.data[0].resposta4);
  respostasConstrutor.push(this.navParams.data[0].resposta5);

  this.verificaRepostas(respostasConstrutor, 1);

  this.barChart = new Chart(this.barCanvas.nativeElement, {

      type: 'bar',
          data: {
              labels: ["Não", "Insuficiente", "Parcialmente", "Sim"],
              datasets: [{
                  label: 'Porcentagem de respostas',
                  data: [this.respConstrutor.respNao, this.respConstrutor.respInsuficiente, this.respConstrutor.respParcialmente, this.respConstrutor.respSim],
                  backgroundColor: [
                      'rgba(255, 99, 132, 0.2)',
                      'rgba(54, 162, 235, 0.2)',
                      'rgba(255, 206, 86, 0.2)',
                      'rgba(75, 192, 192, 0.2)'
                  ],
                  borderColor: [
                      'rgba(255,99,132,1)',
                      'rgba(54, 162, 235, 1)',
                      'rgba(255, 206, 86, 1)',
                      'rgba(75, 192, 192, 1)'
                  ],
                  borderWidth: 1
              }]
          },
          options: {
              scales: {
                  yAxes: [{
                      ticks: {
                          beginAtZero: true
                      }
                  }],
                  xAxes: [{
                      gridLines: {
                          offsetGridLines: true
                      }
                  }]
              }
          }

  });
}

graficoOrganizador(){
  let respostasOrganizador: any[] = [];
  respostasOrganizador.push(this.navParams.data[1].resposta1);
  respostasOrganizador.push(this.navParams.data[1].resposta2);
  respostasOrganizador.push(this.navParams.data[1].resposta3);
  respostasOrganizador.push(this.navParams.data[1].resposta4);
  respostasOrganizador.push(this.navParams.data[1].resposta5);

  this.verificaRepostas(respostasOrganizador, 2);

  this.barChartOrgan = new Chart(this.barCanvasOrgan.nativeElement, {

      type: 'bar',
          data: {
              labels: ["Não", "Insuficiente", "Parcialmente", "Sim"],
              datasets: [{
                  label: 'Porcentagem de respostas',
                  data: [this.respOrganizador.respNao, this.respOrganizador.respInsuficiente, this.respOrganizador.respParcialmente, this.respOrganizador.respSim],
                  backgroundColor: [
                      'rgba(255, 99, 132, 0.2)',
                      'rgba(54, 162, 235, 0.2)',
                      'rgba(255, 206, 86, 0.2)',
                      'rgba(75, 192, 192, 0.2)'
                  ],
                  borderColor: [
                      'rgba(255,99,132,1)',
                      'rgba(54, 162, 235, 1)',
                      'rgba(255, 206, 86, 1)',
                      'rgba(75, 192, 192, 1)'
                  ],
                  borderWidth: 1
              }]
          },
          options: {
              scales: {
                  yAxes: [{
                      ticks: {
                          beginAtZero: true
                      }
                  }],
                  xAxes: [{
                      gridLines: {
                          offsetGridLines: true
                      }
                  }]
              }
          }

  });
}

graficoProgramador(){
  let respostasProgramador: any[] = [];
  respostasProgramador.push(this.navParams.data[2].resposta1);
  respostasProgramador.push(this.navParams.data[2].resposta2);
  respostasProgramador.push(this.navParams.data[2].resposta3);
  respostasProgramador.push(this.navParams.data[2].resposta4);
  respostasProgramador.push(this.navParams.data[2].resposta5);

  this.verificaRepostas(respostasProgramador, 3);

  this.barChartProg = new Chart(this.barCanvasProg.nativeElement, {

      type: 'bar',
          data: {
              labels: ["Não", "Insuficiente", "Parcialmente", "Sim"],
              datasets: [{
                  label: 'Porcentagem de respostas',
                  data: [this.respProgramador.respNao, this.respProgramador.respInsuficiente, this.respProgramador.respParcialmente, this.respProgramador.respSim],
                  backgroundColor: [
                      'rgba(255, 99, 132, 0.2)',
                      'rgba(54, 162, 235, 0.2)',
                      'rgba(255, 206, 86, 0.2)',
                      'rgba(75, 192, 192, 0.2)'
                  ],
                  borderColor: [
                      'rgba(255,99,132,1)',
                      'rgba(54, 162, 235, 1)',
                      'rgba(255, 206, 86, 1)',
                      'rgba(75, 192, 192, 1)'
                  ],
                  borderWidth: 1
              }]
          },
          options: {
              scales: {
                  yAxes: [{
                      ticks: {
                          beginAtZero: true
                      }
                  }],
                  xAxes: [{
                      gridLines: {
                          offsetGridLines: true
                      }
                  }]
              }
          }

  });
}

graficoLider(){
  let respostasLider: any[] = [];
  respostasLider.push(this.navParams.data[3].resposta1);
  respostasLider.push(this.navParams.data[3].resposta2);
  respostasLider.push(this.navParams.data[3].resposta3);
  respostasLider.push(this.navParams.data[3].resposta4);
  respostasLider.push(this.navParams.data[3].resposta5);

  this.verificaRepostas(respostasLider, 4);

  this.barChartLider = new Chart(this.barCanvasLider.nativeElement, {

      type: 'bar',
          data: {
              labels: ["Não", "Insuficiente", "Parcialmente", "Sim"],
              datasets: [{
                  label: 'Porcentagem de respostas',
                  data: [this.respLider.respNao, this.respLider.respInsuficiente, this.respLider.respParcialmente, this.respLider.respSim],
                  backgroundColor: [
                      'rgba(255, 99, 132, 0.2)',
                      'rgba(54, 162, 235, 0.2)',
                      'rgba(255, 206, 86, 0.2)',
                      'rgba(75, 192, 192, 0.2)'
                  ],
                  borderColor: [
                      'rgba(255,99,132,1)',
                      'rgba(54, 162, 235, 1)',
                      'rgba(255, 206, 86, 1)',
                      'rgba(75, 192, 192, 1)'
                  ],
                  borderWidth: 1
              }]
          },
          options: {
              scales: {
                  yAxes: [{
                      ticks: {
                          beginAtZero: true
                      }
                  }],
                  xAxes: [{
                      gridLines: {
                          offsetGridLines: true
                      }
                  }]
              }
          }

  });
}

graficos(){
  this.graficoConstrutor();
  this.graficoOrganizador();
  this.graficoProgramador();
  this.graficoLider();
}

}