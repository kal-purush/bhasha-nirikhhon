import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HospitalselecterComponent } from './hospitalselecter.component';

describe('HospitalselecterComponent', () => {
  let component: HospitalselecterComponent;
  let fixture: ComponentFixture<HospitalselecterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HospitalselecterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HospitalselecterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});