import { Component, OnInit } from '@angular/core';
import { Recipe } from '../recipe.model';

@Component({
  selector: 'app-recipes-list',
  templateUrl: './recipes-list.component.html',
  styleUrls: ['./recipes-list.component.scss']
})
export class RecipesListComponent implements OnInit {
  recipes:Recipe[]=[
    new Recipe('A Test Recipe','This is simply a test','https://lh3.googleusercontent.com/BOuh1_D9cyTHgNv5g8j3HRwW8BzPvAq3i9WV9xgFvm1hxQ_IlhnvZVGS4aAUPRGaElFzHpV6lQ8XWRPGcluZhM0InX0EUB0oh3vU=w220-h220-c-rw-v1-e365') ];
  constructor() { }

  ngOnInit(): void {
  }

}