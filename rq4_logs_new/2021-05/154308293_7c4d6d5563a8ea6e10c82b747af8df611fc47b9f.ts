import { ControlContainer, NgForm } from '@angular/forms';
import { Component, OnInit } from '@angular/core';

import { parse } from '@firestitch/date';

import { FieldComponent } from '../../field/field.component';


@Component({
  selector: 'fs-field-render-date',
  templateUrl: 'field-render-date.component.html',
  viewProviders: [ { provide: ControlContainer, useExisting: NgForm } ]
})
export class FieldRenderDateComponent extends FieldComponent implements OnInit {

  public ngOnInit(): void {
    super.ngOnInit();

    this.field.data.value = parse(this.field.data.value);
  }

}