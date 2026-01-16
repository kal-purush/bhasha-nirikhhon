import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ListCarDiscountsComponent } from './list-car-discounts.component';

describe('ListCarDiscountsComponent', () => {
  let component: ListCarDiscountsComponent;
  let fixture: ComponentFixture<ListCarDiscountsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ListCarDiscountsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ListCarDiscountsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});