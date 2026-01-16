import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PreTripInspectionComponent } from './pre-trip-inspection.component';

describe('PreTripInspectionComponent', () => {
  let component: PreTripInspectionComponent;
  let fixture: ComponentFixture<PreTripInspectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PreTripInspectionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PreTripInspectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});