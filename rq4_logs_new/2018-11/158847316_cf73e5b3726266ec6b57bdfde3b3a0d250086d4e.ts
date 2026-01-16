import {Component, EventEmitter, OnInit, Output} from '@angular/core';

@Component({
  selector: 'app-add-car',
  templateUrl: './add-car.component.html',
  styleUrls: ['./add-car.component.scss']
})
export class AddCarComponent implements OnInit {

  carName = '';
  carYear = 2017;
  // это специальный тип для эмулаяции события, в <> данные которые он будет емитеть наружу
  // Output это грубо говоря передача данных наружу

  // tslint:disable-next-line:no-output-on-prefix no-output-rename
  @Output('onAddCar') carEmmiter = new EventEmitter<{name: string, year: number}>();

  constructor() { }

  ngOnInit() {
  }

  addCar() {
    this.carEmmiter.emit({
      name: this.carName,
      year: this.carYear
    });

    // этих два поля нужны что б в инпут после добавления сбрасывать на занчения по умолчанию
    this.carName = '';
    this.carYear = 2017;
  }
}