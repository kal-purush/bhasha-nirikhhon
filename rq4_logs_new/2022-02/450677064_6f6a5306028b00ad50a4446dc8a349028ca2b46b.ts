import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EvaluationEntrepriseInfoComponent } from './evaluation-entreprise-info.component';

describe('EvaluationEntrepriseInfoComponent', () => {
  let component: EvaluationEntrepriseInfoComponent;
  let fixture: ComponentFixture<EvaluationEntrepriseInfoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EvaluationEntrepriseInfoComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EvaluationEntrepriseInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});