/* tslint:disable:no-unused-variable */

import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';

import { LawListComponent } from './law-list.component';

describe('DashboardComponent', () => {
  let component: LawListComponent;
  let fixture: ComponentFixture<LawListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [LawListComponent]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LawListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});