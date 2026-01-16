import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';
import { Countries } from '@app/store/reducer';

export const API_KEY = 'ef81016019d54f48885dad8513c8dc59';
export type NewsCategories = 'Entertainment' | 'General' | 'Health' | 'Science' | 'Sport' | 'Technology';
const routes = {
  topNews: (c: NewsContext) => `/top-headlines?country=${c.country}&apiKey=${API_KEY}`,
  newsByCategory: (c: NewsContext) => `/top-headlines?category=${c.category}&country=${c.country}&apiKey=${API_KEY}`,
};

export interface NewsContext {
  category?: NewsCategories;
  country: Countries
}

export interface NewsApiResponse {
  status: string;
  totalResults: number;
  articles: NewsArticle[];
}

export interface NewsArticle {
  author: string;
  content: string;
  description: string;
  publishedAt: Date;
  source: { id: number, name: string };
  title: string;
  url: string;
  urlToImage: string;
}

@Injectable({
  providedIn: 'root',
})
export class NewsApiService {
  constructor(private httpClient: HttpClient) { }

  public getTopNews (context: NewsContext): Observable<NewsArticle[] | string> {
    return this.httpClient.get(routes.topNews(context)).pipe(
      map((body: NewsApiResponse) => {
        console.log(body);
        return body.articles;
      }),
      catchError(() => of('Error, could not load data :-('))
    );
  }
}