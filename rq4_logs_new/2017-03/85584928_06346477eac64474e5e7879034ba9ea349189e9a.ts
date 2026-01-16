import { Component } from '@angular/core';
import { NavController, NavParams ,ViewController} from 'ionic-angular';
import * as firebase from 'firebase';
/*
  Generated class for the FriendRequest page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-friend-request',
  templateUrl: 'friend-request.html'
})
export class FriendRequestPage {
item:any;
 arr:any[]=[];
  constructor(public navCtrl: NavController, private viewCtrl: ViewController,
    private params: NavParams ) {
   this.arr=[];
      var array=[]
  firebase.database().ref('/userProfile').child(firebase.auth().currentUser.uid).child("friendRequests").once('value').then(function(snapshot) {

   snapshot.forEach(function(userSnapshot) {
       var username = userSnapshot.val();
       var uid=userSnapshot.key;
       array.push({user:username,
       	uid:uid});

   });

});
  this.arr=array
  }


  ionViewDidLoad() {
   // console.log('ionViewDidLoad FriendRequestPage');
  }
close(){
    this.viewCtrl.dismiss();
  }
  accept(x,i){
  	var current=firebase.database().ref('/userProfile').child(firebase.auth().currentUser.uid);
  	var xUser=firebase.database().ref('/userProfile').child(x)
  	current.child("friends").child(x).set(x);
  	xUser.child("friends").child(firebase.auth().currentUser.uid).set(firebase.auth().currentUser.uid)
  	this.arr.splice(i,1);
  	firebase.database().ref('/userProfile').child(firebase.auth().currentUser.uid).child("friendRequests").child(x).remove();
  }
}



