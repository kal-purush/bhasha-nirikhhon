import { Injectable } from '@angular/core';
import { Adapter } from '@app/core/adapter';
import { ProductCategoryDto } from './product-category.dto';
import { ProductCategory } from './product-category.model';

@Injectable()
export class ProductCategoryAdapter implements Adapter<ProductCategory, ProductCategoryDto> {
  toModel(data: ProductCategoryDto): ProductCategory {
    return new ProductCategory(data.id, data.name, data.description);
  }
  toDto(data: ProductCategory): ProductCategoryDto {
    // no particular mapping currently
    return { ...data };
  }
}