import { Component, OnInit, ViewChild, ElementRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Chart, ChartDataset, ChartOptions } from 'chart.js';
import 'chartjs-adapter-date-fns';
import { PriceService } from '../../services/price.service';

@Component({
  selector: 'app-price-tracker',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './price-tracker.component.html',
  styleUrls: ['./price-tracker.component.css']
})
export class PriceTrackerComponent implements OnInit {
  @ViewChild('chartCanvas', { static: true }) chartCanvas!: ElementRef;
  chart!: Chart;
  purchasedCases: any[] = [];
  colorMap: Map<string, string> = new Map();
  predefinedColors: string[] = [
    '#FF5733', '#33FF57', '#3357FF', '#FF33A1', '#A133FF',
    '#33FFF5', '#F5FF33', '#FF8C33', '#8C33FF', '#33FF8C'
  ];

  constructor(private priceService: PriceService) { }

  ngOnInit() {
    this.fetchAllHistory();
  }

  fetchAllHistory() {
    this.priceService.getAllHistory().subscribe(data => {
      this.purchasedCases = data;
      this.assignColors();
      this.createChart();
    });
  }

  assignColors() {
    let colorIndex = 0;
    this.purchasedCases.forEach(entry => {
      if (!this.colorMap.has(entry.name)) {
        this.colorMap.set(entry.name, this.predefinedColors[colorIndex % this.predefinedColors.length]);
        colorIndex++;
      }
    });
  }

  createChart() {
    if (this.chart) {
      this.chart.destroy();
    }

    const ctx = this.chartCanvas.nativeElement.getContext('2d');

    const datasets: ChartDataset<'line'>[] = [];
    const groupedData = new Map<string, { x: number; y: number }[]>();

    this.purchasedCases.forEach(entry => {
      const entryDate = new Date(entry.date).getTime();
      if (!groupedData.has(entry.name)) {
        groupedData.set(entry.name, []);
      }
      groupedData.get(entry.name)?.push({ x: entryDate, y: entry.price });
    });

    groupedData.forEach((values, key) => {
      values.sort((a, b) => a.x - b.x);
      datasets.push({
        label: key,
        data: values,
        borderColor: this.colorMap.get(key) as string,
        backgroundColor: this.colorMap.get(key) as string,
        pointRadius: 5,
        pointBackgroundColor: this.colorMap.get(key) as string,
        pointBorderColor: this.colorMap.get(key) as string,
        borderWidth: 3,
        tension: 0.4
      });
    });

    this.chart = new Chart(ctx, {
      type: 'line',
      data: { datasets },
      options: {
        responsive: true,
        plugins: {
          legend: { display: true, position: 'top', labels: { font: { size: 14 } } },
          tooltip: { enabled: true }
        },
        scales: {
          x: {
            type: 'time',
            time: { unit: 'minute', tooltipFormat: 'HH:mm' },
            ticks: { autoSkip: false, maxTicksLimit: 12, reverse: false }
          },
          y: { beginAtZero: false }
        }
      } as ChartOptions<'line'>
    });
  }
}