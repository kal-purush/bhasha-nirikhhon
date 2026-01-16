import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';

/**
 * Generated class for the StatsPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@IonicPage()
@Component({
  selector: 'page-stats',
  templateUrl: 'stats.html',
})
export class StatsPage {
  guageOptions: any 

  constructor(public navCtrl: NavController, public navParams: NavParams) {
   this.guageOptions = {  
        chart: {
            type: 'solidgauge',
            marginTop: 50
        },
    
        title: {
            text: 'Activity',
            style: {
                fontSize: '24px'
            }
        },
    
        // tooltip: {
        //     borderWidth: 0,
        //     backgroundColor: 'none',
        //     shadow: false,
        //     style: {
        //         fontSize: '16px'
        //     },
        //     // pointFormat: '{series.name}<br><span style="font-size:2em; color: {point.color}; font-weight: bold">{point.y}%</span>',
        //     positioner: function (labelWidth) {
        //         return {
        //             x: 200 - labelWidth / 2,
        //             y: 180
        //         };
        //     }
        // },
    
        pane: {
            startAngle: 0,
            endAngle: 360,
            background: [{ // Track for Move
                outerRadius: '112%',
                innerRadius: '88%',
                backgroundColor: '#F7484E',
                    // .setOpacity() 
                    // .get(),
                borderWidth: 0
            }, { // Track for Exercise
                outerRadius: '87%',
                innerRadius: '63%',
                backgroundColor:'#98EEC1',
                    // .setOpacity(0.3)
                    // .get(),
                borderWidth: 0
            }, { // Track for Stand
                outerRadius: '62%',
                innerRadius: '38%',
                backgroundColor: '#FF9B00',
                    // .setOpacity(0.3)
                    // .get(),
                borderWidth: 0
            }]
        },
    
        yAxis: {
            min: 0,
            max: 100,
            lineWidth: 0,
            tickPositions: []
        },
    
        plotOptions: {
            solidgauge: {
                dataLabels: {
                    enabled: false
                },
                linecap: 'round',
                stickyTracking: false,
                rounded: true
            }
        },
    
        series: [{
            name: 'Move',
            data: [{
                color: '#F7484E',
                radius: '112%',
                innerRadius: '88%',
                y: 80
            }]
        }, {
            name: 'Exercise',
            data: [{
                color: '#98EEC1',
                radius: '87%',
                innerRadius: '63%',
                y: 65
            }]
        }, {
            name: 'Stand',
            data: [{
                color: '#FF9B00',
                radius: '62%',
                innerRadius: '38%',
                y: 50
            }]
        }]
   }  






/**
 * In the chart load callback, add icons on top of the circular shapes
 */
  //  function callback() {

  //       // Move icon
  //       this.renderer.path(['M', -8, 0, 'L', 8, 0, 'M', 0, -8, 'L', 8, 0, 0, 8])
  //           .attr({
  //               'stroke': '#303030',
  //               'stroke-linecap': 'round',
  //               'stroke-linejoin': 'round',
  //               'stroke-width': 2,
  //               'zIndex': 10
  //           })
  //           .translate(190, 26)
  //           .add(this.series[2].group);
    
  //       // Exercise icon
  //       this.renderer.path(['M', -8, 0, 'L', 8, 0, 'M', 0, -8, 'L', 8, 0, 0, 8,
  //               'M', 8, -8, 'L', 16, 0, 8, 8])
  //           .attr({
  //               'stroke': '#ffffff',
  //               'stroke-linecap': 'round',
  //               'stroke-linejoin': 'round',
  //               'stroke-width': 2,
  //               'zIndex': 10
  //           })
  //           .translate(190, 61)
  //           .add(this.series[2].group);

  //       // Stand icon
  //       this.renderer.path(['M', 0, 8, 'L', 0, -8, 'M', -8, 0, 'L', 0, -8, 8, 0])
  //           .attr({
  //               'stroke': '#303030',
  //               'stroke-linecap': 'round',
  //               'stroke-linejoin': 'round',
  //               'stroke-width': 2,
  //               'zIndex': 10
  //           })
  //           .translate(190, 96)
  //           .add(this.series[2].group);
  // };
  
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad StatsPage');
  }

}