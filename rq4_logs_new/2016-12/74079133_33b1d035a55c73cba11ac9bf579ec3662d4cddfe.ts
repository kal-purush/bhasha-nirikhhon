/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';
import {ChartistModule} from 'angular2-chartist';

import { HeightExperimentGraphComponent } from './height-experiment-graph.component';

describe('HeightExperimentGraphComponent', () => {
  let component: HeightExperimentGraphComponent;
  let fixture: ComponentFixture<HeightExperimentGraphComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HeightExperimentGraphComponent ],
      imports: [
        ChartistModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HeightExperimentGraphComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});