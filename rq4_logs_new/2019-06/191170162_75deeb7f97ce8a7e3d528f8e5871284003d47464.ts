import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MakeCustomerAcountComponent } from './make-customer-acount.component';

describe('MakeCustomerAcountComponent', () => {
  let component: MakeCustomerAcountComponent;
  let fixture: ComponentFixture<MakeCustomerAcountComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MakeCustomerAcountComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MakeCustomerAcountComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});