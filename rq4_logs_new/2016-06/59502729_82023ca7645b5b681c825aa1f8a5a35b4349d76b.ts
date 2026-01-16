import { Component, Input, ElementRef }   from '@angular/core';

import { LineChart } from './line-chart';
import { AbstractChartComponent } from './abstract-chart.component';
import { LineChartOptions } from './interfaces';
import { DataSet } from './data-set';

@Component({
    selector: 'line-chart',
    template: `<div id="chart"></div>`
})
export class LineChartComponent extends AbstractChartComponent {
    @Input() data: DataSet;
    @Input() options: LineChartOptions;

    constructor(private _elementRef: ElementRef) {
        super();
    }

    drawChart() {
        let div = this._elementRef.nativeElement.firstElementChild;
        let chart = new LineChart();
        this.options.selector = div;
        chart.draw(this.data, this.options);
    }

}