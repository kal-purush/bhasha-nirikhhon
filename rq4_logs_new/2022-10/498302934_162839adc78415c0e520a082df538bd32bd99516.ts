import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormControl, FormGroup } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
  selector: 'app-add-dialog',
  templateUrl: './add-dialog.component.html',
  styleUrls: ['./add-dialog.component.css']
})
export class AddDialogComponent implements OnInit {
  form: FormGroup;
  
  constructor(public dialogRef: MatDialogRef<AddDialogComponent>,
              @Inject(MAT_DIALOG_DATA) public data: DialogData,
              private formBuilder: FormBuilder ) { }

  ngOnInit(): void {
    this.form = new FormGroup({
      id:        new FormControl(),
      fullname:  new FormControl(),
      shortname: new FormControl(),
      nip:       new FormControl(),
      accountno: new FormControl(),
      country:   new FormControl(),
      city:      new FormControl(),
      street:    new FormControl(),
      zip:       new FormControl()
    });
  }

  onNoClick(): void {
    this.dialogRef.close();
  }
}

export interface DialogData {
  animal: string;
  name: string;
}