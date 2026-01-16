import { Component, ViewChild } from '@angular/core';
import { ViewController, Slides } from 'ionic-angular';

@Component({
  templateUrl: 'build/pages/intro-slides/intro-slides.html',
})

export class IntroSlidesPage {

  constructor (private viewCtrl: ViewController) {

  }

  @ViewChild('mySlider') slider: Slides;

  goToPrevSlide() {
    this.slider.slidePrev();
  }

  goToNextSlide() {
    this.slider.slideNext();
  }

  closeModal() {
    this.viewCtrl.dismiss();
  }

  slides = [
    {
      title: "Welcome to Daily!",
      description: "Instead of setting yourself a list of goals, set yourself <b>one</b> goal.",
    },
    {
      title: "What is Ionic?",
      description: "<b>Ionic Framework</b> is an open source SDK that enables developers to build high quality mobile apps with web technologies like HTML, CSS, and JavaScript.",
    },
    {
      title: "What is Ionic Cloud?",
      description: "The <b>Ionic Cloud</b> is a cloud platform for managing and scaling Ionic apps with integrated services like push notifications, native builds, user auth, and live updating.",
    }
  ];

}