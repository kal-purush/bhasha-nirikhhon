import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductbrowserComponent } from './productbrowser.component';

describe('ProductbrowserComponent', () => {
  let component: ProductbrowserComponent;
  let fixture: ComponentFixture<ProductbrowserComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProductbrowserComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductbrowserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});