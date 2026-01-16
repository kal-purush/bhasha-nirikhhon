import { Component, OnInit } from '@angular/core';
import { ModalController,NavParams } from '@ionic/angular';
import { LocalNotifications } from '@ionic-native/local-notifications/ngx';

@Component({
  selector: 'app-add-time',
  templateUrl: './add-time.page.html',
  styleUrls: ['./add-time.page.scss'],
})
export class AddTimePage implements OnInit {
  time: any = '';
  constructor(private localNotifications: LocalNotifications, private modalController: ModalController, private nav: NavParams) {
    this.localNotifications.hasPermission().then(d => {
      console.log(d)
    }, err => {
      console.log(err)
      this.localNotifications.requestPermission().then(e => console.log(e), er => {
        console.log(er)
      })
    })
   }
   automate() {
    this.schedule(parseInt(this.time.slice(11, 13)) ,parseInt(this.time.slice(14, 16)));
    this.modalController.dismiss('automated');
   }
   schedule(h, m) {
    this.localNotifications.schedule({
      title:'Hey there!',
      text: `Its time for your ${this.nav.data.name} scene!`,
      trigger: { every: { hour: h, minute: m} },
      foreground: true
    });
    console.log('scheduled');
  }
  ngOnInit() {
  }
  timePick() {
    console.log(this.time)

  }
}