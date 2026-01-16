import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AssignVehicleChartComponent } from './assign-vehicle-chart.component';

describe('AssignVehicleChartComponent', () => {
  let component: AssignVehicleChartComponent;
  let fixture: ComponentFixture<AssignVehicleChartComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AssignVehicleChartComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AssignVehicleChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});