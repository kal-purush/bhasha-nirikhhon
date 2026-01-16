import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DeleteDiscountComponent } from './delete-discount.component';

describe('DeleteDiscountComponent', () => {
  let component: DeleteDiscountComponent;
  let fixture: ComponentFixture<DeleteDiscountComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DeleteDiscountComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DeleteDiscountComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});