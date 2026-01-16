/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { HeightExperimentComponent } from './height-experiment.component';
import {MaterialModule} from '@angular/material';
import {ReactiveFormsModule} from '@angular/forms';

describe('HeightExperimentComponent', () => {
  let component: HeightExperimentComponent;
  let fixture: ComponentFixture<HeightExperimentComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HeightExperimentComponent ],
      imports: [
        MaterialModule.forRoot(),
        ReactiveFormsModule,
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HeightExperimentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});