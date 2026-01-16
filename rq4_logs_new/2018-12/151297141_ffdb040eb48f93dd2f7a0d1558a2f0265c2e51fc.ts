import { Injectable } from '@angular/core';
import { ResourceService, serializeCase ,serializeSnakeCase,QueryOptions } from './resource.service';
import { HttpWrapper } from '../common/barrel';
import { HttpResponse } from '@angular/common/http';
import { urlConfig } from '../environments/config';
import { ProductModel } from '../models/product.model';
import { ProductDto } from 'src/models/product.dto';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable()
export class ProductSearchService extends ResourceService<ProductDto[]> {

    constructor(httpWrapper: HttpWrapper<HttpResponse<ProductDto[]>>) {
        super(
            httpWrapper,
            urlConfig.getFilteredProductsUrl,
            inventoryItemFactory
        )
    }

    getList(paramsObject: any = {}): Observable<ProductDto[]> {
        const serializedSnakeCase =  serializeSnakeCase(paramsObject);
        const paramsQueryString = QueryOptions.toQueryString(serializedSnakeCase);
        const queryUrl = `${this.baseUrl}${paramsQueryString}`;
        console.log(queryUrl);
        return this.httpClient
          .get(`${queryUrl}`)
          .pipe(
            map((list: any[]) => {
                return list.map(serializeCase)
              })
          );
        }
}

const inventoryItemFactory: any = (value: ProductDto[]) => {
    console.log('serializer', value)
    return value.map(item => new ProductModel(item))
}