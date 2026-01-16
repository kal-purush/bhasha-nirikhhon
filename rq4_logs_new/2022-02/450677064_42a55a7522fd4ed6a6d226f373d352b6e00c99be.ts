import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EvaluationResultatComponent } from './evaluation-resultat.component';

describe('EvaluationResultatComponent', () => {
  let component: EvaluationResultatComponent;
  let fixture: ComponentFixture<EvaluationResultatComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EvaluationResultatComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EvaluationResultatComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});