import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MoneyMarketComponent } from './money-market.component';

describe('MoneyMarketComponent', () => {
  let component: MoneyMarketComponent;
  let fixture: ComponentFixture<MoneyMarketComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MoneyMarketComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MoneyMarketComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});