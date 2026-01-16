import { Component } from '@angular/core';
import {
  ComponentFixture,
  TestBed,
  fakeAsync,
  tick,
} from '@angular/core/testing';
import { ReactiveFormsModule, FormControl } from '@angular/forms';
import { MatSelectModule, MatSelect } from '@angular/material/select';
import { By } from '@angular/platform-browser';
import { SelectAllDirective } from './select-all.directive';
import { CommonModule, NgFor } from '@angular/common';
import { OverlayContainer } from '@angular/cdk/overlay';

// 1. Define a type for the values
type TestOption = { id: number; name: string } | string;

@Component({
  template: `
    <mat-form-field>
      <mat-select [formControl]="control" multiple>
        <mat-option appSelectAll [allValues]="allValues">Select All</mat-option>
        <mat-option
          *ngFor="let item of allValues"
          [value]="isObject(item) ? item.id : item"
        >
          {{ isObject(item) ? item.name : item }}
        </mat-option>
      </mat-select>
    </mat-form-field>
  `,
  standalone: true,
  imports: [
    CommonModule,
    MatSelectModule,
    ReactiveFormsModule,
    SelectAllDirective,
    NgFor,
  ],
})
class TestHostComponent {
  control = new FormControl<TestOption[] | number[] | string[]>([]);
  allValues: TestOption[] = [];

  isObject(item: TestOption): item is { id: number; name: string } {
    return (
      typeof item === 'object' &&
      item !== null &&
      'id' in item &&
      'name' in item
    );
  }
}

describe('SelectAllDirective', () => {
  let fixture: ComponentFixture<TestHostComponent>;
  let component: TestHostComponent;
  let overlayContainer: OverlayContainer;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TestHostComponent],
      providers: [OverlayContainer],
    }).compileComponents();

    fixture = TestBed.createComponent(TestHostComponent);
    component = fixture.componentInstance;
    overlayContainer = TestBed.inject(OverlayContainer);
  });

  function openSelect() {
    fixture.detectChanges();
    const matSelect = fixture.debugElement.query(By.directive(MatSelect))
      .componentInstance as MatSelect;
    matSelect.open();
    fixture.detectChanges();
    tick();
    fixture.detectChanges();
  }

  function getOptions() {
    // Options are rendered in the overlay container
    return overlayContainer
      .getContainerElement()
      .querySelectorAll('mat-option');
  }

  it('should select all ids for array of objects with id', fakeAsync(() => {
    component.allValues = [
      { id: 1, name: 'One' },
      { id: 2, name: 'Two' },
      { id: 3, name: 'Three' },
    ];
    fixture.detectChanges();
    openSelect();

    const options = getOptions();
    const selectAllOption = options[0] as HTMLElement;

    // Simulate user selecting "Select All"
    selectAllOption.click();
    fixture.detectChanges();
    tick();

    expect(component.control.value).toEqual([1, 2, 3]);
  }));

  it('should select all values for array of strings', fakeAsync(() => {
    component.allValues = ['A', 'B', 'C'];
    fixture.detectChanges();
    openSelect();

    const options = getOptions();
    const selectAllOption = options[0] as HTMLElement;

    // Simulate user selecting "Select All"
    selectAllOption.click();
    fixture.detectChanges();
    tick();

    expect(component.control.value).toEqual(['A', 'B', 'C']);
  }));

  it('should clear selection when select all is deselected', fakeAsync(() => {
    component.allValues = ['A', 'B', 'C'];
    component.control.setValue(['A', 'B', 'C']);
    fixture.detectChanges();
    openSelect();

    const options = getOptions();
    const selectAllOption = options[0] as HTMLElement;

    // Simulate user deselecting "Select All"
    selectAllOption.click();
    fixture.detectChanges();
    tick();

    expect(component.control.value).toEqual([]);
  }));

  it('should not include "Select All" value in form control', fakeAsync(() => {
    component.allValues = ['A', 'B', 'C'];
    fixture.detectChanges();
    openSelect();

    const options = getOptions();
    const selectAllOption = options[0] as HTMLElement;

    // Simulate user selecting "Select All"
    selectAllOption.click();
    fixture.detectChanges();
    tick();

    expect(component.control.value).not.toContain('Select All');
  }));
});