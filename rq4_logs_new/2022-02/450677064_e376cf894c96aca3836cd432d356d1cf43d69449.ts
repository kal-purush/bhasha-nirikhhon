import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EvaluationFooterComponent } from './evaluation-footer.component';

describe('EvaluationFooterComponent', () => {
  let component: EvaluationFooterComponent;
  let fixture: ComponentFixture<EvaluationFooterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EvaluationFooterComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EvaluationFooterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});