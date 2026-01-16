import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReviewerReviewComponent } from './reviewer-review.component';

describe('ReviewerReviewComponent', () => {
  let component: ReviewerReviewComponent;
  let fixture: ComponentFixture<ReviewerReviewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReviewerReviewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReviewerReviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});