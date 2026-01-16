import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Demo006Component } from './demo006.component';

describe('Demo006Component', () => {
  let component: Demo006Component;
  let fixture: ComponentFixture<Demo006Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Demo006Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Demo006Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});