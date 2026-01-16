import { Injectable } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot, Router } from '@angular/router';
import { Observable, of } from 'rxjs';
import { ArticleService } from 'app/entities/ICAMApi/article/article.service';
import { IArticle } from 'app/shared/model/ICAMApi/article.model';
import { map } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class AddEditArticleResolver implements Resolve<IArticle | null> {
  constructor(private articleService: ArticleService, private router: Router) {}

  resolve(route: ActivatedRouteSnapshot): Observable<IArticle | null> | Observable<null> {
    const id = route.params.id;

    if (id) {
      return this.articleService.find(id).pipe(
        map(resp => {
          if (resp.body) {
            return resp.body;
          } else {
            this.router.navigate(['/backoffice']);
            return null;
          }
        })
      );
    } else {
      return of(null);
    }
  }
}