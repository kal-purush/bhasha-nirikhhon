import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AddenrollmentComponent } from './addenrollment.component';

describe('AddenrollmentComponent', () => {
  let component: AddenrollmentComponent;
  let fixture: ComponentFixture<AddenrollmentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AddenrollmentComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddenrollmentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});