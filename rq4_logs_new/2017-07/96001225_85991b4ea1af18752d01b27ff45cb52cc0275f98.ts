import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';


/**
 * Generated class for the HeatmapPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@IonicPage()
@Component({
  selector: 'page-heatmap',
  templateUrl: 'heatmap.html',
})
export class HeatmapPage {

  chartOptions: any

  constructor(public navCtrl: NavController, public navParams: NavParams) {
    this.chartOptions = {
      chart: {
            type: 'bar'
        },
        title: {
            text: 'Fruit Consumption'
        },
        xAxis: {
            categories: ['Apples', 'Bananas', 'Oranges']
        },
        yAxis: {
            title: {
                text: 'Fruit eaten'
            }
        },
        series: [{
            name: 'Jane',
            data: [1, 0, 4]
        }, {
            name: 'John',
            data: [5, 7, 3]
        }]
    }
  
    
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad HeatmapPage');
  }
}