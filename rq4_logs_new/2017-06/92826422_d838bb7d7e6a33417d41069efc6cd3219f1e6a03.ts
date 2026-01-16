import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Subtab4Component } from './subtab4.component';

describe('Subtab4Component', () => {
  let component: Subtab3Component;
  let fixture: ComponentFixture<Subtab4Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Subtab4Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Subtab4Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});