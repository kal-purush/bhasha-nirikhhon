import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CarBusinessReportsComponent } from './car-business-reports.component';

describe('CarBusinessReportsComponent', () => {
  let component: CarBusinessReportsComponent;
  let fixture: ComponentFixture<CarBusinessReportsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CarBusinessReportsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CarBusinessReportsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});