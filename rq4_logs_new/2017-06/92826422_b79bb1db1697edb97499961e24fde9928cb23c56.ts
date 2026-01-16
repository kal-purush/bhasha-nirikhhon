import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Subtab1Component } from './subtab1.component';

describe('Subtab1Component', () => {
  let component: Subtab1Component;
  let fixture: ComponentFixture<Subtab1Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Subtab1Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Subtab1Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});