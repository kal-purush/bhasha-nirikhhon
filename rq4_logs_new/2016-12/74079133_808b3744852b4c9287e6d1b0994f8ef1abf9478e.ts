/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { ExperimentGraphComponent } from './experiment-graph.component';
import {ChartistModule} from 'angular2-chartist';

describe('ExperimentGraphComponent', () => {
  let component: ExperimentGraphComponent;
  let fixture: ComponentFixture<ExperimentGraphComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ExperimentGraphComponent ],
      imports: [ ChartistModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ExperimentGraphComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});