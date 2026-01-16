import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BillingDetailsPageComponent } from './billing-details-page.component';

describe('BillingDetailsPageComponent', () => {
  let component: BillingDetailsPageComponent;
  let fixture: ComponentFixture<BillingDetailsPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BillingDetailsPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BillingDetailsPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});