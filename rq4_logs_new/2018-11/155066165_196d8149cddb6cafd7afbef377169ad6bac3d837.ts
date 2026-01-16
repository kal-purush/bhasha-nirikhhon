import { Component, OnInit, Inject } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { TeacherModel } from '../core/teacher.model';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

@Component({
  selector: 'cm-teacher-modal-dialog',
  templateUrl: './teacher-modal-dialog.component.html',
  styleUrls: ['./teacher-modal-dialog.component.css']
})
export class TeacherModalDialogComponent implements OnInit {

  teacherForm: FormGroup;
  teacher: TeacherModel;

  constructor(public dialogRef: MatDialogRef<TeacherModalDialogComponent>,
              @Inject(MAT_DIALOG_DATA) public data) {
    console.log(this.data);
    this.teacher = this.data ? this.data.teacher : new TeacherModel();
  }

  ngOnInit() {
    this.createTeacherForm();
  }

  private createTeacherForm() {
    this.teacherForm = new FormGroup({
      first_name: new FormControl(this.teacher.first_name, Validators.required),
      last_name: new FormControl(this.teacher.last_name, Validators.required),
      full_name: new FormControl(this.teacher.full_name)
    });

  }

}