import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BuzzFeedbackComponent } from './buzz-feedback.component';

describe('BuzzFeedbackComponent', () => {
  let component: BuzzFeedbackComponent;
  let fixture: ComponentFixture<BuzzFeedbackComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BuzzFeedbackComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BuzzFeedbackComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});