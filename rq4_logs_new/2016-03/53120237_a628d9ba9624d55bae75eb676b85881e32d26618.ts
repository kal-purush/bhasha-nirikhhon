import {Component, OnInit} from "angular2/core";
import {ArticleService} from "../service/article.service";
import {Router} from 'angular2/router';
import {Article} from "../interface/article";

@Component({
    templateUrl: 'app/layout/news.html',
    providers: [ArticleService]
})
export class NewsComponent implements OnInit{
    
    private articles: Article[];
    private page:number = 0;
    private limit:number = 5;
    private isDisabled:boolean = false;
    constructor(private _articleService: ArticleService,
    private _router: Router){};
    
    getNews() {
        this.page ++;
        this.isDisabled = this.page * this.limit >=10;
        this._articleService.getArticles(this.page, this.limit).then(articles => this.articles = articles);
    }
    ngOnInit() {
        this.getNews();
    }
    gotoArticleDetail(article: Article) {
        let link = ['ArticleDetail', { id: article.id }];
        this._router.navigate(link);
    }
} 