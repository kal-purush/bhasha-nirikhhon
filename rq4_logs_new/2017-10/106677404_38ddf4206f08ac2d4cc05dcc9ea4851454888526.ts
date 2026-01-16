import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import { ToolService } from "../../providers/tool-service";
import { ApiService } from "../../providers/api-service";
/**
 * Generated class for the BookPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-book',
  templateUrl: 'book.html',
})
export class BookPage {

  firstLoaded: boolean = false;
  bookItem: any = null;

  dataType: string = 'chapter';
  chapters: any = [];
  brief: string = '';

  constructor(public navCtrl: NavController, 
              public navParams: NavParams, 
              private api: ApiService,
              private tool: ToolService,
    ) {
      this.bookItem = this.navParams.data;
      console.log(this.bookItem);
  }

  ionViewDidLoad() {
    
  }

  ionViewDidEnter() {
    if (!this.firstLoaded) {
      this.firstLoaded = true;

      this.refresh();
    }
  }

  refresh(): void {
    this.tool.showLoading('加载中...');

    this.bookItem.openID = '187cc0fff2b361dce805e8b0c11c7fedc30a8034';
    this.bookItem.ungz = 1;
    
    this.api.get('you/getBook.php', this.bookItem)
      .then(data => {
        console.log(data);
        this.chapters = data.partArr[0].chapterArr;
        this.brief = data.brief;
        this.tool.hideLoading();
      })
      .catch(error => {
        this.tool.hideLoading();
      });
  }

}