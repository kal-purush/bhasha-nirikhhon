import { Injectable } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot, Router } from '@angular/router';
import { Observable, of, zip } from 'rxjs';
import { ArticleService } from 'app/entities/ICAMApi/article/article.service';
import { IArticle } from 'app/shared/model/ICAMApi/article.model';
import { map, catchError } from 'rxjs/operators';
import { RevisionService } from 'app/entities/ICAMApi/revision/revision.service';
import { IRevision } from 'app/shared/model/ICAMApi/revision.model';

interface IRevisionResolve {
  article: IArticle;
  revision: IRevision | null;
}

@Injectable({ providedIn: 'root' })
export class ReviewArticleResolver implements Resolve<IRevisionResolve | null> {
  constructor(private articleService: ArticleService, private revisionService: RevisionService, private router: Router) {}

  resolve(route: ActivatedRouteSnapshot): Observable<IRevisionResolve | null> | Observable<null> {
    const id = route.params.id;

    if (id) {
      const article$ = this.articleService.find(id).pipe(catchError(() => of(null)));
      const revision$ = this.revisionService.find(id).pipe(catchError(() => of(null)));

      return zip(article$, revision$).pipe(
        map(data => {
          const articleData = data[0];
          const revisionData = data[0];

          if (articleData && articleData.body) {
            return {
              article: articleData.body,
              revision: revisionData ? revisionData.body : null
            };
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