import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CompanySummaryEchartsBarComponent } from './company-summary-echarts-bar.component';

describe('CompanySummaryEchartsBarComponent', () => {
  let component: CompanySummaryEchartsBarComponent;
  let fixture: ComponentFixture<CompanySummaryEchartsBarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CompanySummaryEchartsBarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CompanySummaryEchartsBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});