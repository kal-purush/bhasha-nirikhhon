import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Subtab3Component } from './subtab3.component';

describe('Subtab3Component', () => {
  let component: Subtab3Component;
  let fixture: ComponentFixture<Subtab3Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Subtab3Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Subtab3Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});