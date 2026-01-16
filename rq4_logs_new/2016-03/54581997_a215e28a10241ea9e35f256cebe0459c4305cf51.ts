import { Component, OnInit, EventEmitter } from 'angular2/core';
import { Router, ROUTER_DIRECTIVES } from 'angular2/router';


class Broadcaster extends EventEmitter { }


var loginFlag = false;
@Component({
  selector: 'shop-home',
  templateUrl: 'src/app/Component/home/home.component.html',
  styleUrls: ['src/app/Component/home/home.component.css'],
  directives: [ROUTER_DIRECTIVES],
  outputs: ['homeSelected']
})
export class HomeComponent implements OnInit {
  // heroes: Hero[];
  // selectedHero: Hero;
  // 
  generatedNumber: number = 0;
  homeSelected = new EventEmitter<any>();

  constructor(
    private _router: Router) {
      setInterval(() => {
      this.homeSelected.next('');
      }, 1000);
     }

  ngOnInit() {
    console.log('ng clled --- home');
    // this.getHeroes();
    this.homeSelected.emit('homeeeeeee');
  }
}