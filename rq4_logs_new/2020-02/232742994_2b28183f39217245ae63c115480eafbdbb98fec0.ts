import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ServicetaskbyamountComponent } from './servicetaskbyamount.component';

describe('ServicetaskbyamountComponent', () => {
  let component: ServicetaskbyamountComponent;
  let fixture: ComponentFixture<ServicetaskbyamountComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ServicetaskbyamountComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ServicetaskbyamountComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});