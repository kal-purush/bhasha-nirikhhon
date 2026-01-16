import { Component, Input, OnChanges, SimpleChanges } from "@angular/core";
import * as Highcharts from "highcharts";
import { SeriesOptionsType } from "highcharts";

@Component({
  selector: "app-stacked-column",
  template: '<highcharts-chart [Highcharts]="Highcharts" [options]="chartOptions" [callbackFunction]="chartCallback" style="width: 100%; height: 100%; display: block;"></highcharts-chart>',
})

export class StackedColumnComponent implements OnChanges {

  @Input() series: any = [];
  @Input() title: string = null;
  @Input() subTitle: string = null;

  Highcharts: typeof Highcharts = Highcharts;
  chart!: Highcharts.Chart;
  chartOptions: Highcharts.Options = {
    chart: {
      type: 'column'
    },
    title: {
      text: 'Distribution of Open Data Access Rates by Document Type'
    },
    xAxis: {
      // Updated categories to include specific document types and their totals
      categories: [
        'Bioentity<br>(total = 37,405,521)',
        'Dataset<br>(total = 19,188,757)',
        'Image<br>(total = 3,283,874)',
        'Collection<br>(total = 579,542)',
        'Audiovisual<br>(total = 224,732)',
        'Clinical Trial<br>(total = 150,099)',
        'Other<br>(total = TBD)'
      ],
      title: {
        text: null
      }
    },
    yAxis: {
      min: 0,
      title: {
        text: 'Percentage of Open Data',
        align: 'high'
      },
      labels: {
        overflow: 'justify'
      }
    },
    tooltip: {
      valueSuffix: ' %',
      headerFormat: '<b>{point.x}</b><br/>',
      pointFormat: '{series.name}: {point.y}<br/>Total: {point.total} %'
    },
    plotOptions: {
      column: {
        stacking: 'normal',
        dataLabels: {
          enabled: true,
          format: '{y}%'
        }
      }
    },
    legend: {
      backgroundColor: '#FFFFFF',
      reversed: true
    },
    credits: {
      enabled: false
    },
    series: [{
      type: 'column',
      name: 'Open',
      data: [25, 35, 25, 15, 40, 10, 25],  // Random data to not always total 100%
      color: '#028691'  // Primary color
    }, {
      type: 'column',
      name: 'Restricted',
      data: [45, 30, 35, 45, 20, 60, 20],
      color: '#e4587c'  // Secondary color
    }, {
      type: 'column',
      name: 'Closed',
      data: [15, 25, 30, 20, 25, 20, 40],  // Adding varying percentages
      color: '#515252'  // Color for 'Closed' category
    }, {
      type: 'column',
      name: 'Embargo',
      data: [15, 10, 10, 20, 15, 10, 15],  // New category 'Embargo'
      color: '#fae0d1'  // Another color
    }]
  }



  ngOnChanges(changes: SimpleChanges) {
    // console.log(changes['data']);
    this.updateChart();
  }

  updateChart() {
    if (this.chart) {
      // console.log(this.data);
      this.chart.update({
        // series: this.series as SeriesOptionsType[]
      }, true, true);
    } else {

    }
  }


  chartCallback = (chart: Highcharts.Chart) => {
    this.chart = chart;
    this.updateChart(); // Ensure the chart is updated on initialization

  }

}