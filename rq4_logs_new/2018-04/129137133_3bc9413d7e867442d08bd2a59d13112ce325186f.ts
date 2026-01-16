import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartjsLiveComponent } from './chartjs-live.component';

describe('ChartjsLiveComponent', () => {
  let component: ChartjsLiveComponent;
  let fixture: ComponentFixture<ChartjsLiveComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChartjsLiveComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChartjsLiveComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});