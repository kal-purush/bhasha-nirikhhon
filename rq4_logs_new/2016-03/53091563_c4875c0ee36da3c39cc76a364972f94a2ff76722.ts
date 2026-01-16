import {Page, Modal, NavController, ViewController, NavParams} from 'ionic-angular';

@Page({
  templateUrl: 'build/pages/modal_meetup/modal_meetup.html'
})

export class ModalMeetup {
  invitation: any;
  view: any;

 constructor(viewCtrl: ViewController, params: NavParams) {
   this.view = viewCtrl;
   this.invitation = params.get('meetup');
 }

 dismiss() {
   this.view.dismiss(false);
 }

 invit() {
   this.view.dismiss(true);
 }

}