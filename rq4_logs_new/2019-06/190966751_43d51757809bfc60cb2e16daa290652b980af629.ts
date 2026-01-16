import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalgesicComponent } from './analgesic.component';

describe('AnalgesicComponent', () => {
  let component: AnalgesicComponent;
  let fixture: ComponentFixture<AnalgesicComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AnalgesicComponent ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AnalgesicComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});