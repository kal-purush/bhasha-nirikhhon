import { Component, OnInit } from '@angular/core';
declare var $ :any;

@Component({
  selector: 'app-signin-button',
  templateUrl: './signin-button.component.html',
  styleUrls: []
})
export class SigninButtonComponent implements OnInit {

  constructor() { }

  ngOnInit() {
    $('#peli-signin').popover({
      title: 'Welcome',
      content: '<div class="py-2 peli-signin-popover">' +
                  '<button class="btn btn-sm btn-block btn-outline-dark">Google</button>' +
                '</div>',
      html: true
    });
  }

  signIn = () => {
    $('#peli-signin').popover('show');
  }

}