/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import {LoadSeeyondsDialogComponent, LoadSeeyondsDialogComponent} from './load-seeyonds-dialog.component';

describe('QuoteDialogComponent', () => {
  let component: LoadSeeyondsDialogComponent;
  let fixture: ComponentFixture<LoadSeeyondsDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LoadSeeyondsDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadSeeyondsDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});