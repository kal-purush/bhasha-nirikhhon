import { Component, OnInit } from '@angular/core';
import { ArticleService } from '../entities/ICAMApi/article/article.service';
import { RevisionService } from '../entities/ICAMApi/revision/revision.service';
import { IDropdownSettings } from 'ng-multiselect-dropdown';
import { ActivatedRoute } from '@angular/router';
import { CategoryTreeService } from 'app/entities/ICAMApi/category-tree/category-tree.service';
import { Revision } from 'app/shared/model/ICAMApi/revision.model';
import { Article } from 'app/shared/model/ICAMApi/article.model';
import { ICategoryTree } from 'app/shared/model/ICAMApi/category-tree.model';
import { ArticleTypeService } from 'app/entities/ICAMApi/article-type/article-type.service';
import { IArticleType } from 'app/shared/model/ICAMApi/article-type.model';

@Component({
  selector: 'reviewArticle',
  templateUrl: 'reviewArticle.component.html',
  styleUrls: ['./reviewArticle.scss']
})
export class ReviewArticleComponent implements OnInit {
  categoryDropdown: Array<ICategoryTree> = [];
  categorySelected: Array<any> = [];
  categoryDropdownSettings: IDropdownSettings = {
    singleSelection: false,
    idField: 'id',
    textField: 'itemName',
    enableCheckAll: false
    //selectAllText : 'selecionar tudo',
    //allowSearchFilter : true
  };

  aTypeDropdown: Array<IArticleType> = [];
  aTypeSelected: string | null = null;
  aTypeDropdownSettings: IDropdownSettings = {
    singleSelection: true,
    idField: 'id',
    textField: 'itemName',
    enableCheckAll: false
  };

  peerReviewed = false;
  noPeerReviewed = true; // default

  article: Article = new Article();
  revision: Revision = new Revision();

  constructor(
    private aTypesService: ArticleTypeService,
    private revisionService: RevisionService,
    private articleService: ArticleService,
    private categoryService: CategoryTreeService,
    private route: ActivatedRoute
  ) {}

  ngOnInit(): void {
    //get do ID do artigo passado da página articleList
    let articleId = null;
    this.route.queryParams.subscribe(params => {
      if (params['articleId']) {
        articleId = params['articleId'];
      }
    });

    //carregar dados base do artigo
    if (articleId) {
      this.articleService.getById(articleId).subscribe(res => {
        if (res) {
          Object.assign(this.article, res[0]);
        }
      });
    }

    if (articleId) {
      this.revisionService.getArticleRevision(articleId).subscribe(res => {
        if (res /* && res.length === 0 */) {
          //Não existe revisão
          //Temos de carregar as categorias todas e os tipos de artigos para o dropdown
          this.categoryService.getCategories().subscribe(categories => {
            console.log(categories);
            this.categoryDropdown = categories;
          });
          this.aTypesService.getArticleTypes().subscribe(aTypes => {
            console.log(aTypes);
            this.aTypeDropdown = aTypes;
          });
        } else if (res) {
          //Já existe revisão
          //revisionExists = true;
        }
      });
    }
  }

  checkboxChanged($event: any): void {
    console.log($event);
  }
}