import {Component, OnInit} from '@angular/core';
import {Recipe} from '../../models/recipe.model';
import {ActivatedRoute, Router} from '@angular/router';
import {DarService} from '../../dar/dar.service';
import {User} from '../../models/user.model';
import {Drug} from '../../models/drug.model';
import {RecipesValidationService} from './recipesValidation.service';

@Component({
  // tslint:disable-next-line:component-selector
  selector: 'app-recipesValidation',
  templateUrl: './recipesValidation.component.html',
  styleUrls: ['./recipesValidation.component.css']
})

export class RecipesValidationComponent implements OnInit {
  private recipes: Set<Recipe>;
  private user: User;
  constructor(private router: Router, private route: ActivatedRoute, private service: RecipesValidationService) {
    this.recipes = new Set<Recipe>();
    this.user = new User();
  }
  ngOnInit(): void {
    const user = JSON.parse(localStorage.getItem('user'));
    this.user = user;
    this.service.getRecipes().subscribe(data => {this.recipes = data; });
  }

  validateRecipe(id: BigInteger, email: string) {
      this.service.validateRecipe(id, email).subscribe(result => { this.service.getRecipes().subscribe(data => {this.recipes = data; });
                                                                   alert('Successfully validated recipe!'); });
  }
}