import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CustomerQuotationViewComponent } from './customer-quotation-view.component';

describe('CustomerQuotationViewComponent', () => {
  let component: CustomerQuotationViewComponent;
  let fixture: ComponentFixture<CustomerQuotationViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CustomerQuotationViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CustomerQuotationViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});