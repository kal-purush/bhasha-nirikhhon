import {
  Component,
  DoCheck,
  EventEmitter,
  Input,
  OnInit,
  Output,
  ViewEncapsulation,
  ViewChild
} from '@angular/core';

import { DatePipe } from '@angular/common';

import { DateFilterValue } from './date-filter-value';
import { DateFilterConfig } from './date-filter-config';
import { DateFilterField } from './date-filter-field';
import { DateFilterEvent } from './date-filter-event';

import { cloneDeep, defaults, isEqual } from 'lodash';

import { IMyDrpOptions, IMyDateRangeModel, IMyDateSelected } from 'mydaterangepicker';

/**
 * Sort component
 */
@Component({
  selector: 'ofs-date-filter',
  styleUrls: ['./date-filter.component.scss'],
  templateUrl: './date-filter.component.html',
  providers: [DatePipe]
})
export class DateFilterComponent implements DoCheck, OnInit {

  /**
   * The filter config containing component properties
   */
  @Input() config: DateFilterConfig;

  /**
   * The event emitted when the sort has changed
   */
  @Output('onChange') onChange = new EventEmitter();

  /**
   * Date modal
   */
  @ViewChild('customDateModal') dateModal: any;

  rangeField: DateFilterField = {
    id: 'customRange',
    title: 'Custom range',
    value: () => {
      return {
        after: this.toDate,
        before: this.fromDate
      } as DateFilterValue;
    }
  } as DateFilterField;

  fromDate: Date;
  toDate: Date;
  dateRangePickerOptions: IMyDrpOptions = {
    inline: true
  };

  private currentField: DateFilterField;
  private defaultConfig: DateFilterConfig = {
    visible: true
  } as DateFilterConfig;
  private prevConfig: DateFilterConfig;

  /**
   * The default constructor
   */
  constructor(private datePipe: DatePipe) {
  }

  /**
   *  Setup component configuration upon initialization
   */
  ngOnInit(): void {
    this.setupConfig();
  }

  /**
   *  Check if the component config has changed
   */
  ngDoCheck(): void {
    // Do a deep compare on config
    if (!isEqual(this.config, this.prevConfig)) {
      this.setupConfig();
    }
  }

  /**
   * Set up default config
   */
  protected setupConfig(): void {
    if (this.config !== undefined) {
      defaults(this.config, this.defaultConfig);
    } else {
      this.config = cloneDeep(this.defaultConfig);
    }

    if (this.config && this.config.fields && this.config.fields.length > 0) {
      if (this.currentField === undefined) {
        this.currentField = this.config.fields[0];
      }
    }
  }

  // Actions

  onDateRangeChanged(event: IMyDateRangeModel): void {
    this.fromDate = event.beginJsDate;
    this.toDate = event.endJsDate;
  }

  onDateSelected(event: IMyDateSelected) {
    if (event.type === 1) {
      this.fromDate = event.jsdate;
    } else if (event.type === 2) {
      this.toDate = event.jsdate;
    }
  }

  selectCustomRange() {
    let customRangeField: DateFilterField = {
      id: 'customXRange',
      title: this.datePipe.transform(this.fromDate, 'mediumDate') + ' - ' + this.datePipe.transform(this.toDate.toString(), 'mediumDate'),
      value: () => {
        return {
          after: this.toDate,
          before: this.fromDate
        } as DateFilterValue;
      }
    } as DateFilterField;

    this.selectField(customRangeField);
    this.dateModal.close();
  }

  private selectField(field: DateFilterField): void {
    this.currentField = field;
    this.onChange.emit({
      field: this.currentField
    } as DateFilterEvent);
  }

}