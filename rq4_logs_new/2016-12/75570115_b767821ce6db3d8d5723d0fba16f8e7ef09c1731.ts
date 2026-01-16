/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { TechUsedComponent } from './tech-used.component';

describe('TechUsedComponent', () => {
  let component: TechUsedComponent;
  let fixture: ComponentFixture<TechUsedComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TechUsedComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TechUsedComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});