import { Component, OnInit } from '@angular/core';
import { Validators, FormBuilder, FormGroup } from '@angular/forms';

@Component({
  selector: 'gt-create-grade-group',
  templateUrl: './create-grade-group.component.html',
  styleUrls: ['./create-grade-group.component.css']
})
export class CreateGradeGroupComponent implements OnInit {
  gradeGroupForm: FormGroup;
  constructor(public fb: FormBuilder) {
    this.gradeGroupForm = this.fb.group({
      name: ['', Validators.required],
      weight: ['', Validators.required],
      numGrades: ['']
    });
  }

  ngOnInit() {
  }

}