import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReportConductorComponent } from './report-conductor.component';

describe('ReportConductorComponent', () => {
  let component: ReportConductorComponent;
  let fixture: ComponentFixture<ReportConductorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReportConductorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReportConductorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});