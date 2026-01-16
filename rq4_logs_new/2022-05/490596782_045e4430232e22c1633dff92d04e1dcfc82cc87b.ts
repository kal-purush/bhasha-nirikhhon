import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductCategoryDialogComponent } from './product-category-dialog.component';

describe('ProductCategoryDialogComponent', () => {
  let component: ProductCategoryDialogComponent;
  let fixture: ComponentFixture<ProductCategoryDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProductCategoryDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductCategoryDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});