import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ExpenseSummaryComponent } from './expense-summary.component';

describe('ExpenseSummaryComponent', () => {
  let component: ExpenseSummaryComponent;
  let fixture: ComponentFixture<ExpenseSummaryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ExpenseSummaryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ExpenseSummaryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});