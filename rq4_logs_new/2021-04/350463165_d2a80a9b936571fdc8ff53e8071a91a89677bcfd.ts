import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TechnicalEstimateComponent } from './technical-estimate.component';

describe('TechnicalEstimateComponent', () => {
  let component: TechnicalEstimateComponent;
  let fixture: ComponentFixture<TechnicalEstimateComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TechnicalEstimateComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TechnicalEstimateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});