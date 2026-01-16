import { Component, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'app-product-modal',
  templateUrl: './product-modal.component.html',
  styleUrls: ['./product-modal.component.css']
})
export class ProductModalComponent {
  @Input() product: any;
  @Output() close = new EventEmitter<void>();
  selectedSize: string = '';
  currentImage: string = '';

  ngOnInit() {
    if (this.product) {
      this.currentImage = this.product.imageUrl;
    }
  }

  closeModal() {
    this.close.emit();
  }

  toggleImage() {
    if (this.currentImage === this.product.imageUrl) {
      this.currentImage = this.product.imageUrl2;
    } else {
      this.currentImage = this.product.imageUrl;
    }
  }
}