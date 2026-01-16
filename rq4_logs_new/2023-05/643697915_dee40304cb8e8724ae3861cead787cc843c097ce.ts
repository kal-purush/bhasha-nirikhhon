import 'zone.js/dist/zone';
import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { bootstrapApplication } from '@angular/platform-browser';

@Component({
  selector: 'my-app',
  standalone: true,
  imports: [CommonModule],
  template: `
    <h1> Welcome to my Angular website!</h1>
    <a target="_blank" href={{github}}>Learn more about me and code contributions here </a>
    <h2> Follow my other Social Media </h2>
    <a target="_blank" href="{{linkedIn}}"> LinkedIn Here!</a>
    <h3> </h3>
    <a target="_blank" href="{{twitter_user}}"> Follow my twitter!</a>
  `,
})

export class App {
  name = 'Axel';
  github = 'https://github.com/Axel7771'
  linkedIn = 'https://www.linkedin.com/in/axel-alvarez-8810a01b9/'
  twitter_user = "https://twitter.com/ax7l1"
}

bootstrapApplication(App);