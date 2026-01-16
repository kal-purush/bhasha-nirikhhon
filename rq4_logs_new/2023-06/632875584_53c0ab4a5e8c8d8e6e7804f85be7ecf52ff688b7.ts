import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NavBarv2Component } from './nav-barv2.component';

describe('NavBarv2Component', () => {
  let component: NavBarv2Component;
  let fixture: ComponentFixture<NavBarv2Component>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ NavBarv2Component ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(NavBarv2Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});