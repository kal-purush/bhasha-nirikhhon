import { Component, Input, OnInit, AfterViewInit, OnChanges, ElementRef, ChangeDetectionStrategy  }   from '@angular/core';
import { AbstractChartComponent } from './abstract-chart.component';

import { DataSet } from './data-set';

declare var D3Funnel: any;

@Component({
    selector: 'funnel-chart',
    template: `<div id="chart"></div>`,
})
export class FunnelChartComponent extends AbstractChartComponent  {
    @Input() data: any;
    @Input() options: any;

    constructor(private _elementRef: ElementRef) {
        super();
    }

    drawChart() {
        let div = this._elementRef.nativeElement.firstElementChild;
        let chart: any = new D3Funnel(div);

        chart.draw(this.data, this.options);
    }

}