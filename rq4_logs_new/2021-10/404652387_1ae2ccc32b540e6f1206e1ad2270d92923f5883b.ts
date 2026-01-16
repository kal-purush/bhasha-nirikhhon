import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ConsultCandidatComponent } from './consult-candidat.component';

describe('ConsultCandidatComponent', () => {
  let component: ConsultCandidatComponent;
  let fixture: ComponentFixture<ConsultCandidatComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ConsultCandidatComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConsultCandidatComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});