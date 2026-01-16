import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CrimePredictionComponent } from './crime-prediction.component';

describe('CrimePredictionComponent', () => {
  let component: CrimePredictionComponent;
  let fixture: ComponentFixture<CrimePredictionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CrimePredictionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CrimePredictionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});