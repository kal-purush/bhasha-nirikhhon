import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TrainingListCardComponent } from './training-list-card.component';

describe('TrainingListCardComponent', () => {
  let component: TrainingListCardComponent;
  let fixture: ComponentFixture<TrainingListCardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TrainingListCardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TrainingListCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});