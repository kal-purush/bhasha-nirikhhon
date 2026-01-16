import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SeeTickerPricesComponent } from './see-ticker-prices.component';

describe('SeeTickerPricesComponent', () => {
  let component: SeeTickerPricesComponent;
  let fixture: ComponentFixture<SeeTickerPricesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SeeTickerPricesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SeeTickerPricesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});