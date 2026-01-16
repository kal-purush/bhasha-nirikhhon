import { Component, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { Hero } from "../../constants/hero";
import { HeroService } from "../../services/hero.service";

@Component({
  moduleId: module.id,
  selector: 'my-dashboard',
  templateUrl: 'home-page.component.html',
  styleUrls: ['home-page.component.css']
})

export class HomePageComponent implements OnInit {
  heroes:Hero[] = [];
  
  constructor(private router:Router,
              private heroService:HeroService) {
  }
  
  ngOnInit():void {
    this.heroService.getHeroes()
      .then(heroes => this.heroes = heroes.slice(1, 5));
  }
  
  gotoDetail(hero:Hero):void {
    let link = ['/detail', hero.id];
    this.router.navigate(link);
  }
}