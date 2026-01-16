import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-role-form',
  template: `
    <app-form [form]="form" [formGroup]="form">
      <div class="grid grid-cols-1 gap-4">
        <mat-form-field appearance="outline" class="w-full">
          <mat-label>Name</mat-label>
          <input matInput type="text" required formControlName="name" />
        </mat-form-field>
      </div>
    </app-form>
  `,
})
export class RoleFormComponent implements OnInit {
  form: FormGroup = this.formBuilder.group({
    name: ['', Validators.required],
  });

  constructor(private formBuilder: FormBuilder) {}

  ngOnInit(): void {}
}