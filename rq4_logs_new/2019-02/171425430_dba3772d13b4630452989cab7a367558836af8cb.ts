import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SalaryMaintainComponent } from './salary-maintain.component';

describe('SalaryMaintainComponent', () => {
  let component: SalaryMaintainComponent;
  let fixture: ComponentFixture<SalaryMaintainComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SalaryMaintainComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SalaryMaintainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});