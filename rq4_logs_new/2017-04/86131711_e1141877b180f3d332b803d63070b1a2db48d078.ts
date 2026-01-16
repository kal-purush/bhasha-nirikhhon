import { Component, OnInit } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';

// Importing classes collection and interface
import { Classes } from '../../../../imports/collections';
import { Class } from '../../../../imports/models';

// Importing user collection and interface
import { User } from '../../../../imports/models';
import { Users } from '../../../../imports/collections/users';

import template from './temp-form.component.html';

@Component({
  selector: 'temp-form',
  template
})
export class TempFormComponent implements OnInit {
  addForm: FormGroup;

  constructor(
    private formBuilder: FormBuilder
  ) {}

  public user: User;
  public classDays = [
    { value: 'M', display: 'Monday' },
    { value: 'T', display: 'Tuesday' }
  ];

  ngOnInit() {
    this.addForm = this.formBuilder.group({
      className: ['', Validators.required],
      sectionNumber: [],
      classOwner: ['', Validators.required],
      classDays: this.classDays[0].value,
      classTime: [],
      classCheckInStartTime: [],
      classCheckInEndTime: []
    });
  }

  addClass(): void {
    if (this.addForm.valid) {
      Classes.insert(this.addForm.value);

      this.addForm.reset();
    }
  }
}