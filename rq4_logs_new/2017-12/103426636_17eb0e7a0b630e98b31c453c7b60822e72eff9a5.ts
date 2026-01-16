import { Component, OnInit } from '@angular/core';
import { trigger, state, style, transition, animate } from '@angular/animations';
import { NgForm } from '@angular/forms';

@Component({
  selector: 'app-contact-plain',
  templateUrl: './contact-plain.component.html',
  styleUrls: ['./contact-plain.component.css'],
  animations: [
    trigger('componentState', [
      state('visible', style({
        opacity: 1
      })),
      state('hidden', style({
        opacity: 0
      })),
      transition('hidden => visible', animate(500))
    ]),
  ]
})

export class ContactPlainComponent implements OnInit {
  
    constructor() { }
  
    state = 'hidden';
    timeoutID
  
    onSubmit(form: NgForm) {
      
    }
  
    ngOnInit() {
      this.timeoutID = setTimeout(() => {
        document.getElementById('contact').scrollIntoView({behavior: "smooth", block:"start"})
        this.state = 'visible'
      }, 100);
    }
  
  }
  