import { Component, inject, input, InputSignal, OnInit, signal } from '@angular/core';
import { colorSets, LegendPosition, NgxChartsModule } from '@swimlane/ngx-charts';
import { SensorMeasurementHistory } from '../../../model/measurementHistory.model';
import { ChartService } from '../../../service/chart.service';

@Component({
  selector: 'app-chart',
  standalone: true,
  imports: [NgxChartsModule],
  templateUrl: './chart.component.html',
  styleUrl: './chart.component.scss'
})
export class ChartComponent implements OnInit {

  private readonly chartService: ChartService = inject(ChartService);

  public chartData: InputSignal<SensorMeasurementHistory[]> = input([] as SensorMeasurementHistory[]);

  // view: [number, number] = [370, 300];
  showXAxis = true;
  showYAxis = true;
  gradient = false;
  showLegend = true;
  showXAxisLabel = false;
  showYAxisLabel = false;
  yScaleMin = 0;
  yScaleMax = 40;
  roundDomains = true;
  autoScale = false;
  legendPosition: LegendPosition = LegendPosition.Below
  colorScheme = {
    domain: ['#5AA454', '#A10A28', '#C7B42C', '#AAAAAA']
  };
  referenceLines = [
    {name: "maxAlarm", value: 30.0},
    {name: "minAlarm", value: 18.0},
  ];


  constructor() {
    this.colorScheme = colorSets.find(s => s.name === 'vivid')!;
  }

  ngOnInit() {
    this.prepareChartData();
  }

  prepareChartData() {
    if (this.chartData()) {
      this.yScaleMax = this.chartService.calculateYScaleMax(this.chartData());
      this.yScaleMin = this.chartService.calculateYScaleMin(this.chartData());
      this.referenceLines = this.chartService.calculateReferenceLines(this.chartData());
    }
  }

}