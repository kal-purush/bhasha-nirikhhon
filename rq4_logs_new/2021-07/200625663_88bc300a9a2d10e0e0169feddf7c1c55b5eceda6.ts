import axios, { AxiosInstance } from 'axios';

import { responseHelper } from '@/api/ResponseHelper';

import { ApiResponse } from '@/api/type/ApiResponse';
import { SearchForProductsResponse } from './model/SearchForProductsResponse.model';

class ProductsApi {

    private readonly api: AxiosInstance;

    constructor() {
        this.api = axios.create({
            baseURL: '/api/tesco/product',
        });
    }

    async searchForProducts(searchTerm: string, page: number): Promise<SearchForProductsResponse | Error> {
        try {
            const url = `/search/${searchTerm}/${page}`;

            const response = await this.api.get<ApiResponse<SearchForProductsResponse>>(url);

            return response.data.result;
        }
        catch (error) {
            return responseHelper.handleError(error);
        }
    }
}

export const productsApi = new ProductsApi();