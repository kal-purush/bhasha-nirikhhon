import {bootstrap} from 'angular2/platform/browser';
import {Component} from 'angular2/core';
import {ArticleComponent} from './article.component'
import {Article} from './article'


@Component({
    selector: 'reddit',
    directives: [ArticleComponent],
    template: `
    <form class="ui large form segment">
        <h3 class="ui header">Add a Link</h3>

        <div class="field">
            <label for="title"></label>
            <input name="title" #newtitle>
        </div>
        <div class="field">
            <label for="link">Link: </label>
            <input name="link" #newlink>
        </div>
        <button
        (click)="addArticle(newtitle, newlink)"
        class="ui positive right floated button">
        Submit link
        </button>
    </form>
    <div class="ui grid posts">
        <reddit-article
        *ngFor="#article of sortedArticles()"
        [article]="article">
        </reddit-article>
    </div>
    `
})

class RedditApp {
    articles: Article[];

    constructor() {
        this.articles = [
            new Article('Angular 2', 'http://angular.io', 3),
            new Article('Fullstack', 'http://fullstack.io', 2),
            new Article('Angular Homepage', 'http://angular.io', 1)
        ];
    }

    addArticle(title:HTMLInputElement, link:HTMLInputElement):void {
        var article = new Article(title.value, link.value);
        console.log(`Adding article title: ${article.title} and link: ${article.link}`);
        this.articles.push(article);
        title.value = '';
        link.value = '';
    }

    sortedArticles(): Article[]{
        return this.articles.sort((a:Article, b:Article) => b.votes - a.votes);
    }
}

bootstrap(RedditApp);