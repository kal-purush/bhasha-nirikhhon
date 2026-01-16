import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ViewallSchedulesComponent } from './viewall-schedules.component';

describe('ViewallSchedulesComponent', () => {
  let component: ViewallSchedulesComponent;
  let fixture: ComponentFixture<ViewallSchedulesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ViewallSchedulesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ViewallSchedulesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});