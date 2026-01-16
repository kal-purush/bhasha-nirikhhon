import { Component, Input, OnInit } from '@angular/core';
import { Chart, ChartDataSets, ChartOptions, ChartType } from 'chart.js';
import { Color, Label } from 'ng2-charts';
import { Engagement } from '../engagement/engagement-dto';

@Component({
  selector: 'app-monthly-chart',
  template: `
    <div class="block">
      <canvas
        baseChart
        [datasets]="data"
        [labels]="labels"
        [options]="lineChartOptions"
        [legend]="lineChartLegend"
        [chartType]="lineChartType"
        [plugins]="lineChartPlugins"
      >
      </canvas>
    </div>
  `,
})
export class MonthlyChartComponent implements OnInit {
  @Input() data: ChartDataSets[] = [];

  @Input() labels: Label[] = [];

  lineChartOptions: ChartOptions = {
    responsive: true,
  };

  lineChartLegend = true;
  lineChartType: ChartType = 'line';
  lineChartPlugins = [];
  constructor() {}

  ngOnInit(): void {}

  get monthsFromData(): string[] {
    return [''];
  }
}