import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';
declare var $;

@Component({
    selector: 'app-cash-flow-chart',
    templateUrl: './cash-flow-chart.component.html',
    styles: [`
        .cash-flow-chart {
            height: 325px;
        }
    `]
})
export class CashFlowChartComponent implements OnChanges {
    @Input() user;
    @Input() company;
    @Input() data: any;
    options: {};

    ngOnChanges() {
        if(this.data) { this.drawChart(); }
    }

    drawChart() {
        this.data.total.data.unshift(this.data.actual[this.data.actual.length - 1]);

        this.options = {
            yAxis: {
                title: 'Cash',
                startOnTick: false
            },
            xAxis: {
                type: 'datetime',
                plotBands: [{
                    from: this.data.total.data[1][0],
                    to: this.data.total.data[this.data.total.data.length - 1][0],
                    color: 'rgba(68, 170, 213, .15)'
                }],
                ordinal: false
            },
            rangeSelector: {
                selected: 1,
                inputEnabled: false
            },
            credits: {
                enabled: false
            },
            plotOptions: {
                area: {
                    color: '#a5d6a7',
                    fillColor: 'rgba(65, 135, 63, 0.25)',
                    marker: {
                        radius: 2
                    },
                    lineWidth: 1,
                    states: {
                        hover: {
                            lineWidth: 1
                        }
                    }
                }
            },
            series: [{
                type: 'area',
                id: 'cash',
                name: 'Balance',
                tooltip: {
                    pointFormat: 'Balance: ${point.y:,.2f}',
                    useHTML: true
                },
                data: this.data.actual.concat(this.data.total.data)
            }, {
                type: 'flags',
                onSeries: 'cash',
                data: this.data.flags.reduce((memo, flag) => {
                    let match = memo.filter(f => { return flag.x === f.x; })[0];
                    if (match) {
                        let i = memo.indexOf(match);
                        match.title = 'Multi';
                        match.text = '<p>' + match.text + '</p><br><p>' + flag.description + '</p>';
                        memo[i] = match;
                    } else {
                        memo.push({
                            x: flag.x,
                            title: flag.title,
                            text: flag.description
                        });
                    }
                    return memo;
                }, [])
            }
            ]
        };
    }

    getFakeData () {
        let self = this;
        $.getJSON('https://www.highcharts.com/samples/data/jsonp.php?filename=aapl-c.json&callback=?', function (data) {
            self.data = data;
            self.drawFakeChart();
        });
    }

    drawFakeChart() {
        this.options = {


            rangeSelector: {
                selected: 1
            },

            title: {
                text: 'AAPL Stock Price'
            },

            series: [{
                name: 'AAPL',
                data: this.data,
                tooltip: {
                    valueDecimals: 2
                }
            }]
        }
    }
}