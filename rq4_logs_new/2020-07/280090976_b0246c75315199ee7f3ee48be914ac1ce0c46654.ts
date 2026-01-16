import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TrainingCardDeckComponent } from './training-card-deck.component';

describe('TrainingCardDeckComponent', () => {
  let component: TrainingCardDeckComponent;
  let fixture: ComponentFixture<TrainingCardDeckComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TrainingCardDeckComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TrainingCardDeckComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});