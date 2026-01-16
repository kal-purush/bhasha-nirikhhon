import {
  Component,
  OnInit,
  AfterContentInit,
  AfterViewInit,
  ViewChild,
  ViewChildren,
  ContentChild,
  ElementRef,
  QueryList
} from '@angular/core';
import { JokeComponent } from '../joke/joke.component';
import { Joke } from '@app/shared/model/joke';

@Component({
  selector: 'joke-list',
  templateUrl: './joke-list.component.html'
})
export class JokeListComponent implements OnInit,
  AfterContentInit,
  AfterViewInit {

  jokes: Joke[] = [
    new Joke('What did the cheese say when it looked in the mirror', 'Hello-me (Halloumi)'),
    new Joke('What kind of cheese do you use to disguise a small horse', 'Mask-a-pony (Mascarpone)')
  ];

  @ViewChild(JokeComponent) jokeViewChild: JokeComponent; // this is just the very first one found, not so useful here having multiple.
  @ViewChildren(JokeComponent) jokeViewChildren: QueryList<JokeComponent>;
  @ViewChild('header') headerEl: ElementRef;
  @ContentChild(JokeComponent) jokeContentChild: JokeComponent;

  constructor() {
    console.log(`new - jokeViewChild is ${this.jokeViewChild}`);
    console.log(`new - jokeContentChild is ${this.jokeContentChild}`);
  }

  ngOnInit() { }

  ngAfterContentInit() {
    console.log(`ngAfterContentInit - jokeContentChild(projecting Joke in parent) is ${this.jokeContentChild}`, this.jokeContentChild);
  }

  ngAfterViewInit() {
    console.log(`ngAfterViewInit - jokeViewChild(only very first found) is ${this.jokeViewChild}`, this.jokeViewChild);

    const jokes: JokeComponent[] = this.jokeViewChildren.toArray();
    console.log(`ngAfterViewInit - jokeViewChildren(Jokes in child) is this.jokeViewChildren to array: `, jokes);

    console.log(`ngAfterViewInit - headerEl is ${this.headerEl}`, this.headerEl);
    this.headerEl.nativeElement.textContent = 'Best Joke Machine';
  }

}
