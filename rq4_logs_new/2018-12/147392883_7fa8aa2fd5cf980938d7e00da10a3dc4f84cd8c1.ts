import {Component, Input, OnChanges, OnInit, SimpleChanges} from '@angular/core';

@Component({
  selector: 'app-dynamic-chart',
  templateUrl: './chart-dynamic-component.html',
  styleUrls: ['./chart-dynamic.component.scss']
})
export class ChartDynamicComponent implements OnInit, OnChanges {

  public lineChartData:Array<any> = [
    {data: ['positive', 'neganiv', 'positive', 'neganiv'], label: 'Series A'}
  ];
  public lineChartLabels:Array<any> = ['January', 'February', 'March', 'April', 'May', 'June', 'July'];
  public lineChartOptions:any = {
    responsive: true
  };

  public lineChartLegend:boolean = true;
  public lineChartType:string = 'line';


  constructor() {
  }

  ngOnChanges(changes: SimpleChanges): void {

  }

  ngOnInit() {}

}