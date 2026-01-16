import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AssignVehicleDetailsComponent } from './assign-vehicle-details.component';

describe('AssignVehicleDetailsComponent', () => {
  let component: AssignVehicleDetailsComponent;
  let fixture: ComponentFixture<AssignVehicleDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AssignVehicleDetailsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AssignVehicleDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});