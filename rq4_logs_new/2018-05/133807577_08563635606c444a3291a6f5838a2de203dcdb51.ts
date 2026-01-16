import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DemandeAbsenceComponent } from './demande-absence.component';

describe('DemandeAbsenceComponent', () => {
  let component: DemandeAbsenceComponent;
  let fixture: ComponentFixture<DemandeAbsenceComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DemandeAbsenceComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DemandeAbsenceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});