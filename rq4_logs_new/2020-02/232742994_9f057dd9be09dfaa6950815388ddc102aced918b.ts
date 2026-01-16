import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ExpensedStackedAreaChartComponent } from './expensed-stacked-area-chart.component';

describe('ExpensedStackedAreaChartComponent', () => {
  let component: ExpensedStackedAreaChartComponent;
  let fixture: ComponentFixture<ExpensedStackedAreaChartComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ExpensedStackedAreaChartComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ExpensedStackedAreaChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});