import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { getColor } from '@annoq.common/data/annoq-colors';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { SnpPage } from '../../models/page';
import { FrequencyBucket, SnpAggs } from '../../models/snp-aggs';
import { SnpService } from '../../services/snp.service';

@Component({
  selector: 'annoq-position-stats',
  templateUrl: './position-stats.component.html',
  styleUrls: ['./position-stats.component.scss']
})
export class PositionStatsComponent implements OnInit, OnDestroy {

  snpAggs: SnpAggs;

  posHistogramLineOptions = {
    view: [500, 400],
    legend: false,
    legendPosition: 'below',
    showLabels: true,
    animations: true,
    xAxis: true,
    yAxis: true,
    showYAxisLabel: true,
    showXAxisLabel: true,
    xAxisLabel: 'Position',
    yAxisLabel: 'Annotations',
    timeline: true,
  }


  stats = {
    posHistogramLine: [],
  }

  private _unsubscribeAll: Subject<any>;

  constructor(private snpService: SnpService,) {
    this._unsubscribeAll = new Subject();
  }

  ngOnInit(): void {

    this.snpService.onSnpsAggsChanged
      .pipe(takeUntil(this._unsubscribeAll))
      .subscribe((snpAggs: SnpAggs) => {
        if (snpAggs) {
          console.log(snpAggs)

          this.snpAggs = snpAggs;
          this.drawStats()
        }
      });

  }

  ngOnDestroy(): void {
    this._unsubscribeAll.next();
    this._unsubscribeAll.complete();
  }

  drawStats() {
    const agg = this.snpAggs?.aggs[`${this.snpAggs.field}_frequency`];
    const posHistogramAgg = this.snpAggs?.aggs[`pos_histogram`];

    if (posHistogramAgg?.buckets) {
      this.stats.posHistogramLine = this.snpService.buildAnnotationLine(posHistogramAgg.buckets, 'pos')
    }
  }

}