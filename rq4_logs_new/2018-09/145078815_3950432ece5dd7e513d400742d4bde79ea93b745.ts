import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomerProcessComponent } from './customer-process.component';

describe('CustomerProcessComponent', () => {
  let component: CustomerProcessComponent;
  let fixture: ComponentFixture<CustomerProcessComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CustomerProcessComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CustomerProcessComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});