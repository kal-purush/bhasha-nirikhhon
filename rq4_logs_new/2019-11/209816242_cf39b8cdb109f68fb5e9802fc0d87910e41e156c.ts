import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ShowFeedbackComponent } from './show-feedback.component';

describe('ShowFeedbackComponent', () => {
  let component: ShowFeedbackComponent;
  let fixture: ComponentFixture<ShowFeedbackComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ShowFeedbackComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ShowFeedbackComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});