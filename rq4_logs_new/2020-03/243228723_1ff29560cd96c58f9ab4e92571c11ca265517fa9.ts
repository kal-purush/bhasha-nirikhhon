import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SelfReviewComponent } from './self-review.component';

describe('SelfReviewComponent', () => {
  let component: SelfReviewComponent;
  let fixture: ComponentFixture<SelfReviewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SelfReviewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SelfReviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});