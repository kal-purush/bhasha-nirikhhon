import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LoanProductPageComponent } from './loan-product-page.component';

describe('LoanProductPageComponent', () => {
  let component: LoanProductPageComponent;
  let fixture: ComponentFixture<LoanProductPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LoanProductPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LoanProductPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});