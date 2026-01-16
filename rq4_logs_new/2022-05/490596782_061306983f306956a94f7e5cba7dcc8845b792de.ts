import { Injectable } from '@angular/core';
import { ProductCategoryService } from '@app/product-category/product-category.service';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { ProductCategoryActions } from '../actions';
import { mergeMap, map, catchError, of, shareReplay } from 'rxjs';

@Injectable()
export class ProductCategoryEffect {
  constructor(private actions$: Actions, private productCategoryService: ProductCategoryService) {}

  loadProducts$ = createEffect(() => {
    return this.actions$.pipe(
      ofType(ProductCategoryActions.loadProductCategories),
      mergeMap(() =>
        this.productCategoryService.getAll().pipe(
          map((categories) => ProductCategoryActions.loadProductCategoriesSuccess({ categories })),
          catchError((error) => of(ProductCategoryActions.loadProductCategoriesFailure({ error })))
        )
      )
    );
  });
}