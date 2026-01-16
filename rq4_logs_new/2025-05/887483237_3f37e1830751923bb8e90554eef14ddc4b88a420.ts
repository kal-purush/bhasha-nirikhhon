import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ProductModalComponent } from './product-modal.component';
import { By } from '@angular/platform-browser';
import { TestTranslateModule } from 'tests/test-translate.module';

describe('ProductModalComponent', () => {
  let component: ProductModalComponent;
  let fixture: ComponentFixture<ProductModalComponent>;

  const mockProduct = {
    name: 'Camiseta Punk',
    imageUrl: 'https://example.com/image1.jpg',
    imageUrl2: 'https://example.com/image2.jpg'
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
        declarations: [ProductModalComponent],
        imports: [TestTranslateModule]
    }).compileComponents();

    fixture = TestBed.createComponent(ProductModalComponent);
    component = fixture.componentInstance;
    component.product = mockProduct;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize currentImage with product.imageUrl', () => {
    expect(component.currentImage).toBe(mockProduct.imageUrl);
  });

  it('should toggle image when toggleImage() is called', () => {
    component.toggleImage();
    expect(component.currentImage).toBe(mockProduct.imageUrl2);

    component.toggleImage();
    expect(component.currentImage).toBe(mockProduct.imageUrl);
  });

  it('should emit close event when closeModal() is called', () => {
    spyOn(component.close, 'emit');
    component.closeModal();
    expect(component.close.emit).toHaveBeenCalled();
  });
});