import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ReviewExpectedComponent } from './review-expected.component';

describe('ReviewExpectedComponent', () => {
  let component: ReviewExpectedComponent;
  let fixture: ComponentFixture<ReviewExpectedComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ReviewExpectedComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ReviewExpectedComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});