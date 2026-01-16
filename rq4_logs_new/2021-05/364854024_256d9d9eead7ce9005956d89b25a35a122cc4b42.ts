import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductsFormUpdateComponent } from './products-form-update.component';

describe('ProductsFormUpdateComponent', () => {
  let component: ProductsFormUpdateComponent;
  let fixture: ComponentFixture<ProductsFormUpdateComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProductsFormUpdateComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductsFormUpdateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});