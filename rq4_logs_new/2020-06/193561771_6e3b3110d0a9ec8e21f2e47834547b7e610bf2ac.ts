import { Component, OnInit, ChangeDetectionStrategy, Input, Output, EventEmitter } from '@angular/core';
import {
  ControlContainer,
  FormGroup,
  FormArray,
  FormBuilder,
  Validators,
  NG_VALUE_ACCESSOR,
  NG_VALIDATORS,
  ControlValueAccessor,
  FormControl
} from '@angular/forms';
import { InventoryTargets, InventoryTarget, FlightTarget } from '../store/models';

@Component({
  selector: 'grove-flight-targets',
  template: `
    <fieldset *ngIf="targets">
      <h2>Targets</h2>
      <div *ngFor="let target of targets.controls; let i = index" [formGroup]="target" class="inline-fields">
        <mat-form-field class="target-type" appearance="outline">
          <mat-label>Type</mat-label>
          <mat-select formControlName="type" required #type>
            <mat-option *ngFor="let opt of typeOptions" [value]="opt.id">{{ opt.label }}</mat-option>
          </mat-select>
        </mat-form-field>
        <mat-form-field class="target-code" appearance="outline">
          <mat-label>Target</mat-label>
          <mat-select formControlName="code" required #code>
            <mat-option *ngFor="let opt of codeOptions[i]" [value]="opt.code">{{ opt.label }}</mat-option>
          </mat-select>
        </mat-form-field>
        <div class="target-exclude mat-form-field-wrapper">
          <mat-checkbox>Exclude</mat-checkbox>
        </div>
        <div class="remove-target mat-form-field-wrapper">
          <button mat-icon-button aria-label="Remove target" (click)="removeTarget(i)">
            <mat-icon>delete</mat-icon>
          </button>
        </div>
      </div>
      <div class="add-target">
        <a mat-button routerLink="." (click)="addTarget()"><mat-icon>add</mat-icon> Add a target</a>
      </div>
    </fieldset>
  `,
  styleUrls: ['flight-targets-form.component.scss'],
  providers: [
    { provide: NG_VALUE_ACCESSOR, useExisting: FlightTargetsFormComponent, multi: true },
    { provide: NG_VALIDATORS, useExisting: FlightTargetsFormComponent, multi: true }
  ]
})
export class FlightTargetsFormComponent implements ControlValueAccessor {
  private _targetOptions: InventoryTargets;
  get targetOptions() {
    return this._targetOptions;
  }
  @Input()
  set targetOptions(targetOptions: InventoryTargets) {
    this._targetOptions = targetOptions;
    this.setCodeOptions(this.targets ? (this.targets.value as FlightTarget[]) : []);
  }

  targets: FormArray;
  onChangeFn: (value: any) => void;
  onTouchedFn: (value: any) => void;
  typeOptions = [
    { id: 'episode', label: 'Episode' },
    { id: 'country', label: 'Country' }
  ];
  codeOptions: InventoryTarget[][];

  writeValue(targets: FlightTarget[]) {
    console.log('writeValue', targets);
    this.setCodeOptions(targets);
    this.targets = new FormArray(
      (targets || []).map(t => {
        return new FormGroup({
          type: new FormControl(t.type, Validators.required),
          code: new FormControl(t.code, Validators.required),
          exclude: new FormControl(t.exclude || false)
        });
      })
    );
    this.targets.valueChanges.subscribe(formTargets => {
      console.log('valueCHHANGES', JSON.stringify(formTargets));
      this.onChangeFn(formTargets);
      this.setCodeOptions(formTargets);
    });
  }

  registerOnChange(fn: (value: any) => void) {
    this.onChangeFn = fn;
  }

  registerOnTouched(fn: (value: any) => void) {
    this.onTouchedFn = fn;
  }

  validate({ value }: FormControl) {
    console.log('validate', value);
    return !this.targets || this.targets.valid ? null : { error: 'Some fields are not fullfilled' };
  }

  setCodeOptions(targets: FlightTarget[]) {
    console.log('setCodeOptions', targets);
    this.codeOptions = targets.map(target => {
      return this.filteredTargetOptions(target.type, target.code, targets);
    });
  }

  filteredTargetOptions(type: string, code: string, currentTargets: FlightTarget[]) {
    return this.targetCodesForType(type).filter(opt => {
      if (opt.type == type && opt.code == code) {
        return true;
      } else {
        return !currentTargets.find(t => opt.type == t.type && opt.code == t.code);
      }
    });
  }

  targetCodesForType(type: string) {
    if (this.targetOptions && type === 'episode') {
      return this.targetOptions.episodes.sort((a, b) => a.label.localeCompare(b.label));
    } else if (this.targetOptions && type === 'country') {
      return this.targetOptions.countries.sort((a, b) => a.label.localeCompare(b.label));
    } else {
      return [];
    }
  }
}