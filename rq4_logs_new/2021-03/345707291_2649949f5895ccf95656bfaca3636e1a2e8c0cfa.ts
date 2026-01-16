import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-animated-smoke-text',
  templateUrl: './animated-smoke-text.component.html',
  styleUrls: ['./animated-smoke-text.component.scss']
})
export class AnimatedSmokeTextComponent implements OnInit {
  // TODO: Закончить с анимацией по этому видео
  // https://www.youtube.com/watch?v=ra5qNKNc95U&list=PLHfvxV8-y-JpjZZoBe70UsMmUAEdW-ouu

  constructor() { }

  ngOnInit(): void {
    // now, split text into letters

    const text = document.querySelector('.text');
    text.innerHTML = text.textContent.replace(/\S/g, '<span>$&</span>');
   // now, add active class on hovered <span> tag
    const letters = document.querySelectorAll('span');
    for (let i = 0; i < letters.length; i++) {
      // tslint:disable-next-line:only-arrow-functions
       letters[i].addEventListener('mouseover', function() {
         letters[i].classList.add('active');
       });
     }
  }

}