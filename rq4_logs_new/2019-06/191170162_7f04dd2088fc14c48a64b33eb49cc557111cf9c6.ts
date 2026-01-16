import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductListElementComponent } from './product-list-element.component';

describe('ProductListElementComponent', () => {
  let component: ProductListElementComponent;
  let fixture: ComponentFixture<ProductListElementComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProductListElementComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductListElementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});