import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SurveyFeedbackButtonComponent } from './survey-feedback-button.component';

describe('SurveyFeedbackButtonComponent', () => {
  let component: SurveyFeedbackButtonComponent;
  let fixture: ComponentFixture<SurveyFeedbackButtonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SurveyFeedbackButtonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SurveyFeedbackButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});