import { Component, EventEmitter } from '@angular/core';
import { NavController, NavParams, LoadingController, ViewController, AlertController } from 'ionic-angular';
import { BarService } from '../../../../../_services/bar';
import { Bar } from '../../../../../_models/bar';

@Component({
  selector: 'page-search-bar-event',
  templateUrl: 'search-bar-event.html',
})
export class SearchBarEventPage {

  public inputSearch:string;
  public idEventBar: string;
  private bars: Bar[] = [];
  private bar: Bar;
  showList: boolean = false;
  currentSearch: string;

  constructor(
    navParams: NavParams,
    public navCtrl: NavController,
    private barService: BarService,
    private loadingController: LoadingController,
    private viewCtrl: ViewController,
    private alert: AlertController) {
      this.searchBar(this.inputSearch);
  }


  loading = this.loadingController.create({
    content: 'Buscando estabelecimento...',
    spinner: 'bubbles'
  });

  async searchBar(barname: string) {
    if (barname != undefined && this.currentSearch !== barname.trim()) {
      this.currentSearch = barname;
      if (barname !== undefined && barname.trim() != "") {
        await this.fetchBars(barname);
      } else {
        this.showList = false;
        this.presentAlert('Bar nao encontrado!');
      }
    }
  }

  fetchBars(input: string) {
    this.loading.present();

    this.barService.getByName(input).subscribe(
      bars => {
        this.bars = [];
        this.bars = bars;
        this.showList = this.bars.length !== 0;
        this.loading.dismiss();
      },
      error => {
        this.loading.dismiss();
      }
    );
  }

  presentAlert(message) {
    let alert = this.alert.create({
      title: 'Atenção',
      subTitle: message,
      buttons: ['Entendido']
    });
    alert.present();
  }

  goBack(){
    let defaultBar= new Bar("","",0,0);
    this.viewCtrl.dismiss(defaultBar);
  }

  sendId(bar : Bar){
    this.viewCtrl.dismiss(bar);
  }
}