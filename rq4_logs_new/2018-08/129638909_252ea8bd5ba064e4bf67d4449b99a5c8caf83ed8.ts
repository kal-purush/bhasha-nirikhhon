import { WordPairCategoryModel, WordPairCategoryInterface } from '../models/word-pair-category.model';
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { WordPairCategoryPaginationInterface } from '../models/word-pair-category.model';
import { PaginationUrlParamsModel } from '../models/pagination-url-params.model'
import { toHttpParams } from './vocabulary-http.utils'

@Injectable()
export class WordPairCategoriesHttpService {
  API_URL = '/api/vocabulary-category';

  constructor(private http: HttpClient) { }

  getItems(params: PaginationUrlParamsModel | {} = {}) {
    const httpParams = toHttpParams(params);
    return this.http.get<WordPairCategoryPaginationInterface>(`${this.API_URL}/`,
      {
        params: httpParams
      }
    );
  }

  saveItem(wordPairCategory: WordPairCategoryModel) {
    return this.http.post<WordPairCategoryInterface>(`${this.API_URL}/`, wordPairCategory);
  }

  updateItem(wordPairCategory: WordPairCategoryModel) {
    return this.http.put<WordPairCategoryInterface>(`${this.API_URL}/${wordPairCategory.id}/`, wordPairCategory);
  }

  deleteItem(wordPairCategoryId: number) {
    return this.http.delete(`${this.API_URL}/${wordPairCategoryId}/`);
  }
}