import { Component, Input } from '@angular/core';

@Component({
  selector: '.guess-list',
  templateUrl: './guess-list.component.html',
  styleUrls: ['./guess-list.component.css']
})

export class GuessListComponent {
    @Input() guessList = [];
}