import { Component, ViewChild } from '@angular/core';
import { NavController, Slides, NavParams } from 'ionic-angular';

/*
  Generated class for the CheckinInner page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-checkin-inner',
  templateUrl: 'checkin-inner.html'
})
export class CheckinInnerPage {

	place: any;
	placeContent: any;
	profile: any;
	guests: any;

	@ViewChild('mySlider') slides: Slides;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  	this.placeContent = "0";
  	this.profile = {
  		open: false,
  		checked: false
  	};
  	this.place= {
  		title: 'PlaceName',
  		star: 5,
  		src: 'assets/content/checkinLogo.jpg',
  		count: 50,
  		type: 'qr'
  	};
  	if(navParams.get('type')%2 == 1){
  		this.place.type = 'admin';
  	}
  	this.guests = [
  		{img: 'assets/content/ava.jpg', name: 'Name', table: 2,zone: 'Zone 1', status:'notFriend', id: 1},
  		{img: 'assets/content/ava.jpg', name: 'Name 2', table: 3,zone: 'Zone 2', status:'pending', id: 2},
  		{img: 'assets/content/ava.jpg', name: 'Name 2', table: 2,zone: 'Zone 3', status:'refuse', id: 3},
  		{img: 'assets/content/ava.jpg', name: 'Name 2', table: 3,zone: 'Zone 2', status:'friend', id: 4}
  	]
  }
  getSelfie(){
  	console.log('f*ck your selfie')
  }
  getQR(){
  	console.log('And now it should open the camera i guess')
  }
  openProfile(){
  	this.profile.open = true;
  }
  onSegmentChanged(segmentButton) {
		this.slides.slideTo(segmentButton.value);
	}

  onSlideChanged(slider) {
		this.placeContent = slider.getActiveIndex()+"";

		if(slider.isBeginning()){
			slider.lockSwipeToPrev(true);
		}else{
			slider.lockSwipeToPrev(false);
		}
		if(slider.isEnd()){
			slider.lockSwipeToNext(true);
		}else{
			slider.lockSwipeToNext(false);
		}
	}
  getStars(x: number, y:number){
    let stars = [];

    for(let i = 0; i < 5; i++){
      if(i<y){
        stars[i] = true;
      }else{
        stars[i] = false;
      }
    }
    return stars;
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad CheckinInnerPage');
  }

}