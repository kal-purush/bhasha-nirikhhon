import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductCommentsComponent } from './product-comments.component';

describe('ProductCommentsComponent', () => {
  let component: ProductCommentsComponent;
  let fixture: ComponentFixture<ProductCommentsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProductCommentsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductCommentsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});