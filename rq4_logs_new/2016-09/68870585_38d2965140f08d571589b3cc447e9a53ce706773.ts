import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-about',
  template: `
  <div class="row">
    <div class="col s12">
      <h2 class="deep-orange-text center-align">About These Bananas</h2>
    </div>
  </div>
  <div class="row">
    <div class="col s12 center-align">
      <img class="responsive-img" src="/assets/matt-cover-photo.png">
      <h5>
        Once upon a time, a wild banana suit appeared at <a href="http://reactrally.com">React Rally</a>.
      </h5>
      <h5>
        And proceeded to travel around the JavaScript world, being worn by funny people alike.
      </h5>
      <h5>
        And it was awesome.
      </h5>
      <img class="responsive-img" src="/assets/bensplanation.jpg">
      <p>
        This site was lovingly created by me, <a href="http://twitter.com/ladyleet">@ladyleet</a>, as a demo for LondonJS. It is built in <a href="https://angular.io/">Angular 2.0</a> final release!. It is an <a href="https://cli.angular.io/">Angular-CLI</a> application. Styling used is <a href="http://materializecss.com">MaterializeCSS</a>, and it's deployed using <a href="https://firebase.google.com/">Firebase</a>.
      </p>
    </div>
  </div>
  `,
  styles: []
})
export class AboutComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}