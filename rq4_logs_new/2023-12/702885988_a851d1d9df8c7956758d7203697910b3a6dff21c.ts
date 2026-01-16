import { Component, OnInit } from '@angular/core';
import { IProduct } from 'src/app/page/product/service/product.module';
import { DashboardService } from '../../service/dashboard.service';
import { IDashBoardReq, IDataDthu } from '../../service/dashboard.module';
import * as moment from 'moment';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
})
export class DashboardComponent implements OnInit {
  data: any;
  options: any;
  totalOrder: number = 0;
  totalMoney: number = 0;
  percentOrderComplete: number = 0;
  percentOrderCancel: number = 0;
  dataOrder: any;
  optionsOrder: any;
  products!: IProduct[];

  documentStyle: any;
  textColor!: any;
  textColorSecondary: any;
  surfaceBorder: any;

  dataDthu!: IDataDthu;

  dataDthu7day!: IDataDthu;
  dataDthu28day!: IDataDthu;
  dataDthu6month!: IDataDthu;
  dataDthu1year!: IDataDthu;

  paramDate: any = {};

  constructor(
    private dashboardService: DashboardService
  ) { }

  ngOnInit(): void {
    this.documentStyle = getComputedStyle(document.documentElement);
    this.textColor = this.documentStyle.getPropertyValue('--text-color');
    this.textColorSecondary = this.documentStyle.getPropertyValue(
      '--text-color-secondary'
    );
    this.surfaceBorder = this.documentStyle.getPropertyValue('--surface-border');

    this.dashboardService.doanhThu7day().then(res => {
      this.dataDthu7day = res;
      console.log(res)
      this.setDataPCharBar(res, 'Doanh thu 7 ngay');
    })
    this.textColor



    this.options = {
      maintainAspectRatio: false,
      aspectRatio: 0.6,
      plugins: {
        legend: {
          labels: {
            color: this.textColor,
          },
        },
      },
      scales: {
        x: {
          ticks: {
            color: this.textColorSecondary,
          },
          grid: {
            color: this.surfaceBorder,
          },
        },
        y: {
          ticks: {
            color: this.textColorSecondary,
          },
          grid: {
            color: this.surfaceBorder,
          },
        },
      },
    };

    this.dataOrder = {
      labels: ['A', 'B', 'C'],
      datasets: [
        {
          data: [540, 325, 702],
          backgroundColor: [
            this.documentStyle.getPropertyValue('--blue-500'),
            this.documentStyle.getPropertyValue('--yellow-500'),
            this.documentStyle.getPropertyValue('--green-500'),
          ],
          hoverBackgroundColor: [
            this.documentStyle.getPropertyValue('--blue-400'),
            this.documentStyle.getPropertyValue('--yellow-400'),
            this.documentStyle.getPropertyValue('--green-400'),
          ],
        },
      ],
    };
    this.optionsOrder = {
      plugins: {
        legend: {
          labels: {
            usePointStyle: true,
            color: this.textColor,
          },
        },
      },
    };
  }
  setDataPCharBar(data: IDashBoardReq[], label: string) {
    this.data = {
      labels:
        //  [
        //   'Jan',
        //   'Feb',
        //   'Mar',
        //   'Apr',
        //   'May',
        //   'Jun',
        //   'Jul',
        //   'Aug',
        //   'Sep',
        //   'Oct',
        //   'Nov',
        //   'Dec',
        // ],
        data.map((date) => moment(date.ngayThang).format("MMM DD")),
      datasets: [
        {
          label: label,
          // data: [12, 51, 62, 33, 21, 62, 45, 43, 55, 24, 22, 80],
          data: data.map(date => date.tongDoanhThu),
          fill: true,
          borderColor: this.documentStyle.getPropertyValue('--blue-500'),
          tension: 0.4,
          backgroundColor: 'rgba(255,167,38,0.2)',
        },
      ],
    };
  }

  setDataPCharPie(data: any) { }

  doanhThuWeek() {
    this.dashboardService.doanhThu7day().then(res => {
      this.setDataPCharBar(res, 'Doanh thu 7 ngay');
    })
  }

  doanhThuMonth() {
    this.dashboardService.doanhThu28day().then(res => {
      this.setDataPCharBar(res, 'Doanh thu 28 ngay');
    })
  }
  doanhThu6month() {
    this.dashboardService.doanhThu6month().then(res => {
      this.setDataPCharBar(res, 'Doanh thu 6 tháng');
    })
  }

  doanhThuYear() {
    this.dashboardService.doanhThu1year().then(res => {
      this.setDataPCharBar(res, 'Doanh thu 1 năm');
    })
  }

  doanhThuForDate() {
    if (this.paramDate?.startDate && this.paramDate?.startEnd) {
      this.dashboardService.doanhThuForDate(this.paramDate).then(res => {
        this.setDataPCharBar(res, 'Doanh thu');
      })
    }
  }
}