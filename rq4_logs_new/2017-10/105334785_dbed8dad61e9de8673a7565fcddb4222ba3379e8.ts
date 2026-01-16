import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DispatchPredictionComponent } from './dispatch-prediction.component';

describe('DispatchPredictionComponent', () => {
  let component: DispatchPredictionComponent;
  let fixture: ComponentFixture<DispatchPredictionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DispatchPredictionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DispatchPredictionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});