import { Injectable } from '@angular/core';
import { TwoColumns } from '../models/two-columns';
import { AssotiativeValuesTable } from '../models/assotiative-values';

@Injectable({
  providedIn: 'root'
})
export class DiagramsDataService {
  public pieChartData: TwoColumns[];
  public barChartData: AssotiativeValuesTable;
  public lineChartData: AssotiativeValuesTable;
  public doughnutChartData: AssotiativeValuesTable;
  public radarChartData: AssotiativeValuesTable;
  public polarAreaChartData: TwoColumns[];

  constructor() {
    this.SeedPieChartData();
    this.SeedBarChartData();
    this.SeedLineChartData();
    this.SeedDoughnutChartData();
    this.SeedRadarChartData();
    this.SeedPolarAreaChartData();
  }

  public SeedPieChartData(): void {
    this.pieChartData = [
      { title: 'Sales', value: 30 },
      { title: 'In-Store Sales', value: 50 },
      { title: 'Mail Sales', value: 10 }
    ];
  }

  public SeedBarChartData(): void {
    this.barChartData = {
      setNames: ['Set 1', 'Set 2'],
      rows: [
        { title: '2011', values: [59, 48]},
        { title: '2012', values: [80, 40]},
        { title: '2013', values: [81, 19]},
        { title: '2014', values: [56, 86]},
        { title: '2015', values: [55, 27]},
      ]
    };
  }

  public SeedLineChartData(): void {
    this.lineChartData = {
      setNames: ['Set 1', 'Set 2', 'Set 3'],
      rows: [
        { title: 'January', values: [65, 28, 30]},
        { title: 'February', values: [59, 48, 80]},
        { title: 'March', values: [80, 40, 70]},
        { title: 'April', values: [81, 19, 90]},
        { title: 'May', values: [56, 86, 100]}
      ]
    };
  }

  public SeedDoughnutChartData(): void {
    this.doughnutChartData = {
      setNames: ['Set1', 'Set2', 'Set3'],
      rows: [
        { title: 'Download Sales', values: [350, 50, 150]},
        { title: 'In-Store Sales', values: [150, 250, 120]},
        { title: 'Mail-Order Sales', values: [250, 130, 70]}
      ]
    };
  }

  public SeedRadarChartData(): void {
    this.radarChartData = {
      setNames: ['Set 1', 'Set 2'],
      rows: [
        { title: 'Eating', values: [65, 28]},
        { title: 'Drinking', values: [59, 48]},
        { title: 'Sleeping', values: [80, 40]},
        { title: 'Designing', values: [81, 19]},
        { title: 'Coding', values: [56, 86]},
      ]
    };
  }

  public SeedPolarAreaChartData(): void {
    this.polarAreaChartData = [
      { title: 'Download Sales', value: 300 },
      { title: 'In-Store Sales', value: 500 },
      { title: 'Mail Sales', value: 100 },
      { title: 'Telesales', value: 40 },
      { title: 'Corporate Sales', value: 120 }
    ];
  }
}