import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomerWaitingComponent } from './customer-waiting.component';

describe('CustomerWaitingComponent', () => {
  let component: CustomerWaitingComponent;
  let fixture: ComponentFixture<CustomerWaitingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CustomerWaitingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CustomerWaitingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});