import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams, App } from 'ionic-angular';
import { ToolService } from "../../providers/tool-service";
import { BooksService } from "../../providers/books-service";

/**
 * Generated class for the RecommendPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-recommend',
  templateUrl: 'recommend.html',
})
export class RecommendPage {

  catalogList: any = [];
  hasMore: boolean = true;

  requestParams: any = { server: '服务器4', category: '都市传说', order: '点击排行', page: 1, ungz: 1 };

  constructor(public navCtrl: NavController, 
              public navParams: NavParams,
              private app: App,
              private tool:  ToolService,
              private books: BooksService,
            ) {
    this.requestParams.server = this.navParams.data.server;
    this.requestParams.category = this.navParams.data.category;
    this.requestParams.order = this.navParams.data.order;
  }

  ionViewDidLoad() {
    // console.log('ionViewDidLoad RecommendPage');
    this.loadData();
  }

  gotoBook(book): void {
    this.app.getRootNavs()[0].push('BookPage', book);
  }

  loadData(): Promise<any> {
    if (this.requestParams.page === 1) {
      this.tool.showLoading('加载中...');
    }
    console.log(`page:${this.requestParams.page}`);

    return new Promise(resolve => {
      this.books.getCategories(this.requestParams)
      .then(data => {
        console.log(data);
        // alert(data);
        if (this.requestParams.page === 1) {
          this.catalogList = data.bookArr;
        } else {
          let temp = this.catalogList || [];
          this.catalogList = temp.concat(data.bookArr);
        }
        // console.log(this.catalogList.length);

        this.hasMore = data.bookArr.length == 88;

        resolve(true);

        this.tool.hideLoading();
      })
      .catch(error => {
        console.log(error);
        // alert(error);
        resolve(false);

        this.tool.hideLoading();
      });
    });
    
  }

  doRefresh(e): void {
    this.requestParams.page = 1;

    this.loadData()
      .then(data => {
        e.complete();
      });
  }

  doInfinite(e): void {
    this.requestParams.page ++;
    this.loadData()
      .then(data => {
        e.complete();
      });
  }

}