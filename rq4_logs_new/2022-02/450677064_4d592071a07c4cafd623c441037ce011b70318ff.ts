import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EvaluationMetierComponent } from './evaluation-metier.component';

describe('EvaluationMetierComponent', () => {
  let component: EvaluationMetierComponent;
  let fixture: ComponentFixture<EvaluationMetierComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EvaluationMetierComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EvaluationMetierComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});