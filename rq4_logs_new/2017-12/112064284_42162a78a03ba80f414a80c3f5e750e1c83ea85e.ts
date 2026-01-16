import { Component } from '@angular/core';
import { NavController,NavParams } from 'ionic-angular';
import { AboutGuruPage } from '../about-guru/about-guru';

@Component({
  selector: 'page-blogsdata',
  templateUrl: 'blogsdata.html'
})
export class BlogsDataPage {
  public firstParam;
  public secondParam;
	//blogsdata;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
	  	 this.firstParam = navParams.get("firstPassed");
    this.secondParam = navParams.get("secondPassed");
	}

	/*goToBlogDetail(bdata){
    if (!bdata) bdata = {};
    this.navCtrl.push(AboutGuruPage,{
      firstPassed: "value 1",
      secondPassed: "value 2"
    });
  }*/

}