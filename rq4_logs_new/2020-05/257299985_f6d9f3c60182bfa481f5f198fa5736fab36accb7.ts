import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ListDiscountComponent } from './list-discount.component';

describe('ListDiscountComponent', () => {
  let component: ListDiscountComponent;
  let fixture: ComponentFixture<ListDiscountComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ListDiscountComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ListDiscountComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});