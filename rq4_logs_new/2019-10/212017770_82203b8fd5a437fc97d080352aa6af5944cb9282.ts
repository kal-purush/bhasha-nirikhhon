import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PortfoliDashboardComponent } from './portfoli-dashboard.component';

describe('PortfoliDashboardComponent', () => {
  let component: PortfoliDashboardComponent;
  let fixture: ComponentFixture<PortfoliDashboardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PortfoliDashboardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PortfoliDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});