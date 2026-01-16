import {Component, OnInit, Output} from '@angular/core';
import { Title } from '@angular/platform-browser';



@Component({
  selector: 'app-social-media-icon',
  templateUrl: './social-media-icon.component.html',
  styleUrls: ['./social-media-icon.component.scss']
})
export class SocialMediaIconComponent implements OnInit {

  @Output() outTitle = 'Social Media Icon';



  constructor(private title: Title) {
    title.setTitle('CSS 3D Layered Social Media Icon Hover Effects');
  }

  ngOnInit(): void {
    //! Solution found that page
    //! https://stackoverflow.com/questions/44545163/how-to-add-stylesheet-dynamically-in-angular-2

    const head = document.getElementsByTagName('head')[0];
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://stackpath.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css';
    link.integrity = 'sha384-wvfXpqpZZVQGK6TAh5PVlGOfQNHSoD2xbE+QkPxCAFlNEevoEH3Sl0sibVcOQVnN';
    link.crossOrigin = 'anonymous';
    head.appendChild(link);
  }

}