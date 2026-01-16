import { Component, OnInit } from '@angular/core';
import { ArticleService } from '../../../entities/ICAMApi/article/article.service';
import { Router } from '@angular/router';

@Component({
  selector: 'article-list',
  templateUrl: 'article-list.component.html',
  styleUrls: ['./article-list.scss']
})
export class ArticleListComponent implements OnInit {
  articles: any;
  isFetched = false;

  constructor(private articleService: ArticleService, private router: Router) {}

  getArtictlesToReview(): void {
    this.articleService.getArticlesToReview().subscribe(
      res => {
        this.articles = res;
        this.isFetched = true;
      },
      error => {
        console.log(error);
      }
    );
  }

  ngOnInit(): void {
    this.getArtictlesToReview();
  }

  editArticle(id: number): void {
    if (id >= 0) {
      this.router.navigate(['/backoffice', 'articles', id, 'edit']);
    }
  }

  reviewArticle(id: number): void {
    this.router.navigate(['/backoffice', 'articles', id, 'review']);
  }

  deleteArticle(id: number): void {
    if (confirm(`Tem a certeza que quer apagar o artigo # ${id} ?`)) {
      this.articleService.delete(id).subscribe(
        () => {
          //  Apagado com sucesso
          window.location.reload();
        },
        () => {
          //  error
        }
      );
    }
  }
}