import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';
declare var $;

@Component({
    selector: 'app-exp-by-cat',
    templateUrl: './exp-by-cat.component.html',
    styleUrls: ['./exp-by-cat.component.css']
})
export class ExpByCatComponent implements OnChanges {
    @Input() data: any;
    options: {};

    constructor() {
        this.drawChart();
    }

    ngOnChanges() {
        this.drawChart();
    }

    drawChart() {
        // $.getJSON('https://www.highcharts.com/samples/data/jsonp.php?filename=aapl-c.json&callback=?', function (data) {
        //     // Create the chart
        //     $('#ebc-chart').highcharts('StockChart', {


        //         rangeSelector: {
        //             selected: 1
        //         },

        //         title: {
        //             text: 'AAPL Stock Price'
        //         },

        //         series: [{
        //             name: 'AAPL',
        //             data: data,
        //             tooltip: {
        //                 valueDecimals: 2
        //             }
        //         }]
        //     });
        // });
        // this.options =
        console.log($('.ebc-chart'));
        $('#ebc-chart').highcharts({
            colorAxis: {
                minColor: '#FFFFFF',
                maxColor: '#000000'
            },
            series: [{
                type: 'treemap',
                layoutAlgorithm: 'squarified',
                data: [{
                    name: 'A',
                    value: 6,
                    colorValue: 1
                }, {
                    name: 'B',
                    value: 6,
                    colorValue: 2
                }, {
                    name: 'C',
                    value: 4,
                    colorValue: 3
                }, {
                    name: 'D',
                    value: 3,
                    colorValue: 4
                }, {
                    name: 'E',
                    value: 2,
                    colorValue: 5
                }, {
                    name: 'F',
                    value: 2,
                    colorValue: 6
                }, {
                    name: 'G',
                    value: 1,
                    colorValue: 7
                }]
            }],
            title: {
                text: 'Highcharts Treemap'
            }
        });
    }
}