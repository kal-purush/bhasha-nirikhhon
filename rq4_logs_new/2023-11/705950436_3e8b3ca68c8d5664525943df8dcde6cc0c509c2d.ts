import { Injectable } from '@angular/core';
import { ApiService } from '../../../shared/services/api.service';

@Injectable({
    providedIn: 'root'
})
export class MenuService{

    displayedColumns: string[] = ['no', 'productName', 'price', 'createdAt', 'categoryId', 'productSize', 'status'];

    /**
     * Constructor
     *
     * @param {ApiService} _apiService
     */
    constructor(
        private _apiService: ApiService
    ) {}

    /**
     * Get data list by filter with pagination
     * 
     * @param data 
     */
    getMenuListByFilterWithPagination(data: any) {
        return this._apiService.post(data, 'product/pagination/filter');
    }

    /**
     * Get data by id
     * 
     * @param id 
     */
    getDataById(id: any) {
        return this._apiService.get(`properties/${id}`);
    }

}