import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RatingQuizModalComponent } from './rating-quiz-modal.component';

describe('RatingQuizModalComponent', () => {
  let component: RatingQuizModalComponent;
  let fixture: ComponentFixture<RatingQuizModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RatingQuizModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RatingQuizModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});