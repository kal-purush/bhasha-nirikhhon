import { Injectable } from '@angular/core';
import { ResourceService, serializeCase ,serializeSnakeCase,QueryOptions } from './resource.service';
import { HttpWrapper } from '../common/barrel';
import { HttpResponse } from '@angular/common/http';
import { urlConfig } from '../environments/config';
import { InventoryItemDto } from '../models/inventoryItem.dto'
import { InventoryItemModel } from '../models/inventoryItem.model'
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable()
export class InventoryItemFilterService extends ResourceService<InventoryItemDto[]> {


    constructor(httpWrapper: HttpWrapper<HttpResponse<InventoryItemDto[]>>) {
        super(
            httpWrapper,
            urlConfig.getFilteredItemsUrl,
            inventoryItemFactory
        )
    }
    getList(paramsObject: any = {}): Observable<InventoryItemDto[]> {
        const var1 =  serializeSnakeCase(paramsObject);
        console.log(var1);
        const var2 = QueryOptions.toQueryString(var1);
        console.log(var2);
        const queryUrl = `${this.baseUrl}${var2}`;
        console.log(queryUrl)
        return this.httpClient
          .get(`${queryUrl}`)
          .pipe(
            map((list: any[]) => {
                return list.map(serializeCase)
              })
          );
        }
}

const inventoryItemFactory: any = (value: InventoryItemModel[]) => {
    console.log('serializer', value)
    return value.map(item => new InventoryItemModel())
}