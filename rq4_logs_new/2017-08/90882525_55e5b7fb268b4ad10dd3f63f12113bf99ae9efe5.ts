import { Component, OnInit } from '@angular/core';
import { ShareButton, ShareProvider } from 'ngx-sharebuttons';

@Component({
  selector: 'social-share',
  template: `
  <share-button class="facebook" [button]="fbButton" [url]='appUrl'></share-button>
  `
})

export class ShareComponent implements OnInit {

  appUrl: string = 'http://www.handcricketgame.com';

  fbButton: any;
  ngOnInit(): void {
    // ShareButton(button name [provider], template, classes)
    this.fbButton = new ShareButton(
      ShareProvider.FACEBOOK,
      '<i class="fa fa-facebook"></i>',
      'facebook'
    );
  }
}