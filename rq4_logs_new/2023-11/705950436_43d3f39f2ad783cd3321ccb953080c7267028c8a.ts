import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FeedbackControllerComponent } from './feedback-controller.component';

describe('FeedbackControllerComponent', () => {
  let component: FeedbackControllerComponent;
  let fixture: ComponentFixture<FeedbackControllerComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [FeedbackControllerComponent]
    });
    fixture = TestBed.createComponent(FeedbackControllerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});