/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { NewMealComponent } from './new-meal.component';

describe('NewMealComponent', () => {
  let component: NewMealComponent;
  let fixture: ComponentFixture<NewMealComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NewMealComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NewMealComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});