import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InputSelfFeedbackComponent } from './input-self-feedback.component';

describe('InputSelfFeedbackComponent', () => {
  let component: InputSelfFeedbackComponent;
  let fixture: ComponentFixture<InputSelfFeedbackComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InputSelfFeedbackComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InputSelfFeedbackComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});